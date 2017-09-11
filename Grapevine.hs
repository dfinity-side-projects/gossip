{-# LANGUAGE OverloadedStrings #-}
module Grapevine (Grapevine, grapevineKing, grapevineNoble, grapevinePort, grapevineSize, publish, yell, hear) where

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
import Data.Maybe
import qualified Data.Set as Set
import Data.Set (Set)
import Network.Socket hiding (send, recv)
import Safe
import System.IO

import Kautz

data Message = Hello String | Peerage (M.Map String String) | Blob ByteString deriving (Show, Read)

data Grapevine = Grapevine {
  myNetName :: String,
  myPort :: PortNumber,
  mySock :: Socket,
  blobChan :: BoundedChan ByteString,
  peerage :: MVar (M.Map String SockAddr),
  kingHandle :: Maybe Handle,
  mayStart :: MVar (),
  neighbours :: MVar [Handle],
  seenTables :: MVar (Set ByteString, Set ByteString)
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
  bc <- newBoundedChan 128
  emptyPeerage <- newMVar M.empty
  emptyNeighbours <- newMVar []
  emptySeens <- newMVar (Set.empty, Set.empty)
  notYet <- newEmptyMVar
  pure $ Grapevine {
    myNetName = name,
    myPort = inPort,
    mySock = inSock,
    blobChan = bc,
    peerage = emptyPeerage,
    kingHandle = Nothing,
    mayStart = notYet,
    neighbours = emptyNeighbours,
    seenTables = emptySeens
  }

grapevineSize :: Grapevine -> IO Int
grapevineSize gv = M.size <$> readMVar (peerage gv)

grapevinePort :: Grapevine -> PortNumber
grapevinePort = myPort

grapevineKing :: String -> Int -> IO Grapevine
grapevineKing name port = do
  gv <- newGrapevine name port
  putStrLn $ "PORT = " ++ show (myPort gv)
  void $ forkIO $ kingLoop gv
  pure gv

grapevineNoble :: String -> String -> Int -> IO Grapevine
grapevineNoble king name port = do
  let [host, seedPort] = splitOn ":" king
  gv0 <- newGrapevine name port
  addrInfo <- getAddrInfo Nothing (Just host) (Just seedPort)
  outSock <- reuseMyPort gv0
  connect outSock (addrAddress $ head addrInfo)
  h <- socketToHandle outSock ReadWriteMode
  let gv = gv0 { kingHandle = Just h }
  void $ forkIO $ nobleLoop gv
  void $ forkIO $ handshake gv
  pure gv

handshake :: Grapevine -> IO ()
handshake gv = do
  let Just h = kingHandle gv
  -- 1. Say Hello.
  wire h $ pack $ show $ Hello $ myNetName gv
  -- 2. Read Peerage.
  Just (Peerage m) <- readMay . unpack <$> procure h
  let Just ps = readPeerage m
  void $ swapMVar (peerage gv) ps
  print =<< readMVar (peerage gv)
  socialize gv
  -- 3. Say OK after meshing.
  wire h "OK"
  -- 4. First block should come from king.
  Just (Blob b) <- readMay . unpack <$> procure h
  process gv b
  -- 5. Notify nobleLoop.
  putMVar (mayStart gv) ()

nobleLoop :: Grapevine -> IO ()
nobleLoop gv = do
  (sock, _) <- accept $ mySock gv
  void $ forkIO $ do
    h <- socketToHandle sock ReadWriteMode
    readMVar $ mayStart gv
    forever $ process gv =<< procure h
  nobleLoop gv

kingLoop :: Grapevine -> IO ()
kingLoop gv = do
  (sock, peer) <- accept $ mySock gv
  void $ forkIO $ do
    h <- socketToHandle sock ReadWriteMode
    putStrLn $ show peer
    bs <- procure h
    case readMay $ unpack bs of
      Just (Hello s) -> do
        ps <- takeMVar $ peerage gv
        putMVar (peerage gv) $ M.insert s peer ps
        ns <- takeMVar $ neighbours gv
        putMVar (neighbours gv) $ h:ns
      _ -> putStrLn "BAD HELLO"
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
      pure h
    void $ swapMVar (neighbours gv) $ concat hs
  else if n <= 20 then kautz gv ps 4 1
  else if n <= 30 then kautz gv ps 5 1
  else if n <= 42 then kautz gv ps 6 1
  else if n <= 80 then kautz gv ps 4 2
  else if n <= 108 then kautz gv ps 3 3
  else if n <= 150 then kautz gv ps 5 2
  else if n <= 252 then kautz gv ps 6 2
  else if n <= 320 then kautz gv ps 4 3
  else if n <= 392 then kautz gv ps 7 2
  else if n <= 750 then kautz gv ps 5 3
  else if n <= 1100 then kautz gv ps 10 2
  else if n <= 1512 then kautz gv ps 6 3
  else if n <= 2744 then kautz gv ps 7 3
  else if n <= 4608 then kautz gv ps 8 3
  else if n <= 7290 then kautz gv ps 9 3
  else if n <= 11000 then kautz gv ps 10 3
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
      pure h
    void $ swapMVar (neighbours gv) hs

reportSighting :: Ord a => (Set a, Set a) -> a -> (Set a, Set a)
reportSighting (seen, seen2) h = if Set.size seen == 1024
  then (Set.singleton h, seen)
  else (Set.insert h seen, seen2)

process :: Grapevine -> ByteString -> IO ()
process gv b = do
  (seen, seen2) <- takeMVar $ seenTables gv
  let h = SHA256.hash b
  if Set.member h seen || Set.member h seen2 then
    putMVar (seenTables gv) (seen, seen2)
  else do
    putMVar (seenTables gv) $ reportSighting (seen, seen2) h
    status <- tryWriteChan (blobChan gv) b
    when (not status) $ putStrLn "FULL BUFFER"

publish :: Grapevine -> IO ()
publish gv = do
  ps <- readMVar $ peerage gv
  ns <- readMVar $ neighbours gv
  forConcurrently_ ns $ \h -> do
    wire h $ pack $ show $ Peerage $ M.map show ps
    bs <- procure h
    when (bs /= "OK") $ ioError $ userError "EXPECT OK"
  print ps

isKing :: Grapevine -> Bool
isKing gv = isNothing $ kingHandle gv

yell :: Grapevine -> ByteString -> IO ()
yell gv b = if isKing gv then do
    ns <- readMVar $ neighbours gv
    forConcurrently_ ns $ \h -> wire h $ pack $ show $ Blob b
  else do
    hs <- readMVar $ neighbours gv
    seens <- takeMVar $ seenTables gv
    putMVar (seenTables gv) $ reportSighting seens $ SHA256.hash b
    forConcurrently_ hs $ \h -> wire h b

hear :: Grapevine -> IO ByteString
hear gv = readChan $ blobChan gv
