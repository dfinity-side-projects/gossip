{-# LANGUAGE OverloadedStrings #-}
module Grapevine (Grapevine, grapevineKing, grapevineNoble, publish, yell, hear) where

import Data.ByteString.Char8 (ByteString, pack, unpack)
import qualified Data.ByteString.Char8 as B
import Control.Concurrent hiding (readChan)
import Control.Concurrent.BoundedChan
import Control.Monad
import qualified Crypto.Hash.SHA256 as SHA256
import Data.Char
import Data.IP
import Data.List.Split
import qualified Data.Map.Strict as M
import Network.Socket hiding (send, recv)
import Network.Socket.ByteString
import System.IO
import Text.Read

data Message = Hello String | Peerage (M.Map String String) | Blob ByteString deriving (Show, Read)

data Grapevine = Grapevine {
  myNetName :: String,
  myPort :: PortNumber,
  mySock :: Socket,
  blobChan :: BoundedChan ByteString,
  peerage :: MVar (M.Map String SockAddr),
  seenTable :: MVar (M.Map ByteString [Int])
}

reuseMyPort :: Grapevine -> IO Socket
reuseMyPort gv = do
  outSock <- socket AF_INET Stream 0
  setSocketOption outSock ReusePort 1
  bind outSock $ SockAddrInet (myPort gv) iNADDR_ANY
  pure outSock

newGrapevine :: String -> Int -> IO Grapevine
newGrapevine name port = do
  let sockAddr = SockAddrInet (fromIntegral port) iNADDR_ANY
  inSock <- socket AF_INET Stream 0
  setSocketOption inSock ReusePort 1
  bind inSock sockAddr
  listen inSock 128
  inPort <- socketPort inSock
  bc <- newBoundedChan 20
  st <- newMVar M.empty
  Grapevine name inPort inSock bc st <$> newMVar M.empty

grapevineKing :: String -> Int -> IO Grapevine
grapevineKing name port = do
  gv <- newGrapevine name port
  putStrLn $ "PORT = " ++ show (myPort gv)
  void $ forkIO $ kingLoop gv
  pure gv

grapevineNoble :: String -> String -> Int -> IO Grapevine
grapevineNoble king name port = do
  let
    [host, seedPort] = splitOn ":" king
  gv <- newGrapevine name port
  addrInfo <- getAddrInfo Nothing (Just host) (Just seedPort)
  outSock <- reuseMyPort gv
  connect outSock (addrAddress $ head addrInfo)
  void $ send outSock $ pack $ show $ Hello name
  bs <- recv outSock 4096
  when (bs /= "OK") $ ioError $ userError "The King is dead?"
  close outSock
  void $ forkIO $ nobleLoop gv
  pure gv

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
        void $ send sock "OK"
      _ -> void $ send sock "E_BADHELLO"
    close sock
  kingLoop gv

readPeerage :: M.Map String String -> Maybe (M.Map String SockAddr)
readPeerage m = Just $ M.map f m where
  f s = SockAddrInet (read port) host where
    [ip4, port] = splitOn ":" s
    host = toHostAddress $ read ip4

report :: Handle -> ByteString -> IO ()
report h s = void $ B.hPut h s

wire :: Handle -> ByteString -> IO ()
wire h s = do
  let n = B.length s
  when (n > 2 * 1024 * 1024) $ ioError $ userError "artifact too large!"
  let ds = map (chr . (`mod` 256) . div n) $ (256^) <$> [3, 2, 1, 0 :: Int]
  forM_ ds $ hPutChar h
  B.hPut h s
  status <- B.hGet h 4096
  when (status /= "OK") $ putStrLn $ "BUG: " ++  show status

procure :: Handle -> IO ByteString
procure h = do
  ds <- unpack <$> B.hGet h 4
  let n = sum $ zipWith (*) (ord <$> ds) $ (256^) <$> [3, 2, 1, 0 :: Int]
  when (n > 2 * 1024 * 1024) $ ioError $ userError "artifact too large!"
  B.hGet h n

nobleLoop :: Grapevine -> IO ()
nobleLoop gv = do
  (sock, _) <- accept $ mySock gv
  void $ forkIO $ do
    han <- socketToHandle sock ReadWriteMode
    bs <- procure han
    case readMaybe $ unpack bs of
      Just (Peerage m) -> do
        case readPeerage m of
          Nothing -> report han "E_BADPEERAGE"
          Just ps -> do
            void $ swapMVar (peerage gv) ps
            print =<< readMVar (peerage gv)
            report han "OK"
      Just (Blob b) -> do
        seen <- takeMVar $ seenTable gv
        let h = SHA256.hash b
        case M.lookup h seen of
          Nothing -> do
            putMVar (seenTable gv) $ M.insert h [] seen
            status <- tryWriteChan (blobChan gv) b
            report han $ if status then "OK" else "E_FULL"
          Just _ -> do
            putMVar (seenTable gv) seen
            putStrLn $ "old: " ++ show h
            report han "OK"
      _ -> report han "E_BADTYPE"
    hClose han
  nobleLoop gv

publish :: Grapevine -> IO ()
publish gv = do
  ps <- readMVar $ peerage gv
  forM_ (M.elems ps) $ \sock -> do
    tmpSock <- socket AF_INET Stream 0
    connect tmpSock sock
    h <- socketToHandle tmpSock ReadWriteMode
    wire h $ pack $ show $ Peerage $ M.map show ps
    hClose h
  print ps

yell :: Grapevine -> ByteString -> IO ()
yell gv b = do
  ps <- readMVar $ peerage gv
  seen <- takeMVar $ seenTable gv
  putMVar (seenTable gv) $ M.insert (SHA256.hash b) [] seen
  forM_ (M.assocs ps) $ \(s, sock) -> when (s /= myNetName gv) $ do
    tmpSock <- socket AF_INET Stream 0
    connect tmpSock sock
    h <- socketToHandle tmpSock ReadWriteMode
    wire h $ pack $ show $ Blob b
    hClose h

hear :: Grapevine -> IO ByteString
hear gv = readChan $ blobChan gv
