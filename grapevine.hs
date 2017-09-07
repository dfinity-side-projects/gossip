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
import Data.List
import Data.List.Split
import qualified Data.Map.Strict as M
import Network.Socket hiding (send, recv)
import Network.Socket.ByteString
import Safe
import System.IO

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
  bc <- newBoundedChan 128
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
  close outSock
  void $ forkIO $ nobleLoop gv
  pure gv

kingLoop :: Grapevine -> IO ()
kingLoop gv = do
  (sock, peer) <- accept $ mySock gv
  void $ forkIO $ do
    putStrLn $ show peer
    bs <- recv sock 4096
    case readMay $ unpack bs of
      Just (Hello s) -> do
        ps <- takeMVar $ peerage gv
        putMVar (peerage gv) $ M.insert s peer ps
      _ -> putStrLn "BAD HELLO"
    close sock
  kingLoop gv

readPeerage :: M.Map String String -> Maybe (M.Map String SockAddr)
readPeerage m = Just $ M.map f m where
  f s = SockAddrInet (read port) host where
    [ip4, port] = splitOn ":" s
    host = toHostAddress $ read ip4

wire :: Handle -> ByteString -> IO ()
wire h s = do
  let n = B.length s
  when (n > 2 * 1024 * 1024) $ ioError $ userError "artifact too large!"
  let ds = map (chr . (`mod` 256) . div n) $ (256^) <$> [3, 2, 1, 0 :: Int]
  forM_ ds $ hPutChar h
  B.hPut h s
  hFlush h

procure :: Handle -> IO ByteString
procure h = do
  ds <- unpack <$> B.hGet h 4
  let n = sum $ zipWith (*) (ord <$> ds) $ (256^) <$> [3, 2, 1, 0 :: Int]
  when (n > 2 * 1024 * 1024) $ ioError $ userError "artifact too large!"
  B.hGet h n

socialize :: Grapevine -> IO ()
socialize gv = do
  ps <- readMVar $ peerage gv
  let n = M.size ps
  if n < 10 then do
    hs <- forConcurrently (M.assocs ps) $ \(s, sock) -> if s == myNetName gv then pure [] else pure <$> do
      tmp <- socket AF_INET Stream 0
      connect tmp sock
      h <- socketToHandle tmp ReadWriteMode
      wire h $ pack $ show Link
      pure h
    void $ swapMVar (neighbours gv) $ concat hs
  else if n <= 20 then kautz gv ps 4 1
  else if n <= 30 then kautz gv ps 5 1
  else if n <= 42 then kautz gv ps 6 1
  else if n <= 80 then kautz gv ps 4 2
  else if n <= 108 then kautz gv ps 3 3
  else if n <= 150 then kautz gv ps 5 2
  else undefined

kautz :: Grapevine -> M.Map String SockAddr -> Int -> Int -> IO ()
kautz gv ps m n = let
  sz = (m + 1)*m^n
  lim = M.size ps
  i = M.findIndex (myNetName gv) ps
  os = filter (/= i) $ nub $ (`mod` lim) <$> (kautzOut m n i ++ if lim + i < sz then kautzOut m n (lim + i) else [])
  in do
    when (sz > lim * 2) $ ioError $ userError "BUG! Kautz graph too big!"
    hs <- forConcurrently ((`M.elemAt` ps) <$> os) $ \(_, sock) -> do
      tmp <- socket AF_INET Stream 0
      connect tmp sock
      h <- socketToHandle tmp ReadWriteMode
      wire h $ pack $ show Link
      pure h
    void $ swapMVar (neighbours gv) hs


toKautz :: Int -> Int -> Int -> [Int]
toKautz m n i = f [] n i where
  f [] r k = f [k `mod` (m + 1)] (r - 1) (k `div` (m + 1))
  f acc@(h:_) 0 k = (if k >= h then k + 1 else k):acc
  f acc@(h:_) r k = f (x:acc) (r - 1) (k `div` m) where
    y = k `mod` m
    x = if y >= h then y + 1 else y

fromKautz :: Int -> [Int] -> Int
fromKautz m as = f 0 as where
  f i (x:y:rest) = f (i * m + (if x > y then x - 1 else x)) (y:rest)
  f i [y] = i * (m + 1) + y
  f _ _   = undefined

{-
prop_kautz :: Int -> Int -> Bool
prop_kautz m n = s == (fromKautz m . toKautz m n <$> s) where s = [0..(m+1)*m^n]
-}

kautzOut :: Int -> Int -> Int -> [Int]
kautzOut m n i = fromKautz m . (t ++) . pure <$> filter (/= last t) [0..m]
  where t = tail $ toKautz m n i

kautzIn :: Int -> Int -> Int -> [Int]
kautzIn m n i = fromKautz m . (:s) <$> filter (/= head s) [0..m]
  where s = init $ toKautz m n i

process :: Grapevine -> ByteString -> IO ()
process gv b = do
  seen <- takeMVar $ seenTable gv
  let h = SHA256.hash b
  case M.lookup h seen of
    Nothing -> do
      putMVar (seenTable gv) $ M.insert h [] seen
      status <- tryWriteChan (blobChan gv) b
      when (not status) $ putStrLn "FULL BUFFER"
    Just _ -> putMVar (seenTable gv) seen

nobleLoop :: Grapevine -> IO ()
nobleLoop gv = do
  (sock, _) <- accept $ mySock gv
  void $ forkIO $ do
    h <- socketToHandle sock ReadWriteMode
    bs <- procure h
    case readMay $ unpack bs of
      Just Link -> do  -- TODO: Check sender `elem` kautzIn.
        forever $ process gv =<< procure h
      Just (Peerage m) -> do
        case readPeerage m of
          Nothing -> putStrLn "BAD PEERAGE"
          Just ps -> do
            void $ swapMVar (peerage gv) ps
            print =<< readMVar (peerage gv)
            socialize gv
      Just (Blob b) -> do
        process gv b
      _ -> putStrLn "BAD MESSAGE"
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
