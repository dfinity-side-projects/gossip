{-# LANGUAGE OverloadedStrings #-}
module Grapevine (Grapevine, grapevineKing, grapevineNoble, publish, yell, hear) where

import Data.ByteString.Char8 (ByteString, pack, unpack)
import qualified Data.ByteString.Char8 as B
import Control.Concurrent hiding (readChan)
import Control.Concurrent.BoundedChan
import Control.Concurrent.Async
import Control.Monad
import qualified Crypto.Hash.SHA256 as SHA256
import Data.Char
import Data.IP
import Data.List.Split
import qualified Data.Map.Strict as M
import Data.Maybe
import Network.Socket hiding (send, recv)
import Network.Socket.ByteString
import Safe
import System.IO
import Text.Read

data Message = Hello String | Peerage (M.Map String String) | Link | Blob ByteString deriving (Show, Read)

data Grapevine = Grapevine {
  isKing :: Bool,
  myNetName :: String,
  myPort :: PortNumber,
  mySock :: Socket,
  blobChan :: BoundedChan ByteString,
  peerage :: MVar (M.Map String SockAddr),
  neighbours :: MVar [Handle],
  seenTable :: MVar (M.Map ByteString [Int])
}

reuseMyPort :: Grapevine -> IO Socket
reuseMyPort gv = do
  outSock <- socket AF_INET Stream 0
  setSocketOption outSock ReusePort 1
  bind outSock $ SockAddrInet (myPort gv) iNADDR_ANY
  pure outSock

newGrapevine :: Bool -> String -> Int -> IO Grapevine
newGrapevine royal name port = do
  let sockAddr = SockAddrInet (fromIntegral port) iNADDR_ANY
  inSock <- socket AF_INET Stream 0
  setSocketOption inSock ReusePort 1
  bind inSock sockAddr
  listen inSock 128
  inPort <- socketPort inSock
  bc <- newBoundedChan 20
  emptyPeerage <- newMVar M.empty
  emptyNeighbours <- newMVar []
  Grapevine royal name inPort inSock bc emptyPeerage emptyNeighbours <$> newMVar M.empty

grapevineKing :: String -> Int -> IO Grapevine
grapevineKing name port = do
  gv <- newGrapevine True name port
  putStrLn $ "PORT = " ++ show (myPort gv)
  void $ forkIO $ kingLoop gv
  pure gv

grapevineNoble :: String -> String -> Int -> IO Grapevine
grapevineNoble king name port = do
  let
    [host, seedPort] = splitOn ":" king
  gv <- newGrapevine False name port
  addrInfo <- getAddrInfo Nothing (Just host) (Just seedPort)
  outSock <- reuseMyPort gv
  connect outSock (addrAddress $ head addrInfo)
  void $ send outSock $ pack $ show $ Hello name
  -- bs <- recv outSock 4096
  -- when (bs /= "OK") $ ioError $ userError "The King is dead?"
  close outSock
  void $ forkIO $ nobleLoop gv
  pure gv

data StatusCode = OK | EFull | EBadPeerage | EBadType | EUnknown deriving (Enum, Bounded, Show, Eq)

kingLoop :: Grapevine -> IO ()
kingLoop gv = do
  (sock, peer) <- accept $ mySock gv
  void $ forkIO $ do
    putStrLn $ show peer
    bs <- recv sock 4096
    case readMaybe $ unpack bs of
      Just (Hello s) -> do
        ps <- takeMVar $ peerage gv
        putMVar (peerage gv) $ M.insert s peer ps
        -- void $ send sock "OK"
      _ -> pure () -- void $ send sock "E_BADHELLO"
    close sock
  kingLoop gv

readPeerage :: M.Map String String -> Maybe (M.Map String SockAddr)
readPeerage m = Just $ M.map f m where
  f s = SockAddrInet (read port) host where
    [ip4, port] = splitOn ":" s
    host = toHostAddress $ read ip4

report :: Handle -> StatusCode -> IO ()
report h s = do
  void $ hPutChar h $ chr $ fromEnum s
  hFlush h

getStatus :: Handle -> IO StatusCode
getStatus h = do
  i <- ord <$> hGetChar h
  pure $ fromMaybe EUnknown $ toEnumMay i

wire :: Handle -> ByteString -> IO ()
wire h s = do
  let n = B.length s
  when (n > 2 * 1024 * 1024) $ ioError $ userError "artifact too large!"
  let ds = map (chr . (`mod` 256) . div n) $ (256^) <$> [3, 2, 1, 0 :: Int]
  forM_ ds $ hPutChar h
  B.hPut h s
  status <- getStatus h
  when (status /= OK) $ putStrLn $ "BUG: " ++ show status

procure :: Handle -> IO ByteString
procure h = do
  ds <- unpack <$> B.hGet h 4
  let n = sum $ zipWith (*) (ord <$> ds) $ (256^) <$> [3, 2, 1, 0 :: Int]
  when (n > 2 * 1024 * 1024) $ ioError $ userError "artifact too large!"
  B.hGet h n

socialize :: Grapevine -> IO ()
socialize gv = do
  ps <- readMVar $ peerage gv
  hs <- forConcurrently (M.assocs ps) $ \(s, sock) -> if s == myNetName gv then pure [] else pure <$> do
    tmp <- socket AF_INET Stream 0
    connect tmp sock
    h <- socketToHandle tmp ReadWriteMode
    wire h $ pack $ show Link
    pure h
  void $ swapMVar (neighbours gv) $ concat hs

process :: Grapevine -> Handle -> ByteString -> IO ()
process gv han b = do
  seen <- takeMVar $ seenTable gv
  let h = SHA256.hash b
  case M.lookup h seen of
    Nothing -> do
      putMVar (seenTable gv) $ M.insert h [] seen
      status <- tryWriteChan (blobChan gv) b
      report han $ if status then OK else EFull
    Just _ -> do
      putMVar (seenTable gv) seen
      report han OK

nobleLoop :: Grapevine -> IO ()
nobleLoop gv = do
  (sock, _) <- accept $ mySock gv
  void $ forkIO $ do
    h <- socketToHandle sock ReadWriteMode
    bs <- procure h
    case readMaybe $ unpack bs of
      Just Link -> do
        report h OK
        forever $ process gv h =<< procure h
      Just (Peerage m) -> do
        case readPeerage m of
          Nothing -> report h EBadPeerage
          Just ps -> do
            void $ swapMVar (peerage gv) ps
            print =<< readMVar (peerage gv)
            socialize gv
            report h OK
      Just (Blob b) -> do
        process gv h b
      _ -> report h EBadType
    hClose h
  nobleLoop gv

publish :: Grapevine -> IO ()
publish gv = do
  ps <- readMVar $ peerage gv
  forConcurrently_ (M.elems ps) $ \sock -> do
    tmpSock <- socket AF_INET Stream 0
    connect tmpSock sock
    h <- socketToHandle tmpSock ReadWriteMode
    wire h $ pack $ show $ Peerage $ M.map show ps
    hClose h
  print ps

yell :: Grapevine -> ByteString -> IO ()
yell gv b = if isKing gv then do
    ps <- readMVar $ peerage gv
    forConcurrently_ (M.assocs ps) $ \(s, sock) -> when (s /= myNetName gv) $ do
      tmpSock <- socket AF_INET Stream 0
      connect tmpSock sock
      h <- socketToHandle tmpSock ReadWriteMode
      wire h $ pack $ show $ Blob b
      hClose h
  else do
    hs <- readMVar $ neighbours gv
    seen <- takeMVar $ seenTable gv
    putMVar (seenTable gv) $ M.insert (SHA256.hash b) [] seen
    forConcurrently_ hs $ \h -> wire h b

hear :: Grapevine -> IO ByteString
hear gv = readChan $ blobChan gv
