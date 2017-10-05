{-# LANGUAGE OverloadedStrings #-}
module Grapevine (Grapevine, grapevineKing, grapevineNoble,
  grapevinePort, grapevineSize, grapevineTable,
  publish, yell, hear, getPeerage, getStats, putStats, htmlNetStats) where

import Data.ByteString.Char8 (ByteString, pack, unpack)
import qualified Data.ByteString.Char8 as B
import Control.Concurrent hiding (readChan)
import Control.Concurrent.BoundedChan as BChan
import Control.Exception
import Control.Monad
import qualified Crypto.Hash.SHA256 as SHA256
import Data.Char
import Data.IP
import Data.List.Split
import qualified Data.Map.Strict as M
import Data.Maybe
import qualified Data.Set as S
import Data.Set (Set)
import Network.Socket hiding (send, recv)
import Safe
import System.IO

import Kautz

data Message = Hello String | Peerage Bool (M.Map String String)
  | Predecessor String | Successor String
  | Edict ByteString deriving (Show, Read)

data Grapevine = Grapevine {
  myNetName :: Maybe String,
  myPort :: PortNumber,
  mySock :: Socket,
  blobCh :: BoundedChan ByteString,
  statsCh :: BoundedChan (String, ByteString),
  peerage :: MVar (M.Map String SockAddr),
  subjects :: MVar (M.Map String Handle),
  pres :: MVar (S.Set String),
  sucs :: MVar (M.Map String (BoundedChan ByteString, Handle)),
  netStats :: MVar (M.Map String Int),
  seenTables :: MVar [Set ByteString],
  isMeshed :: MVar Bool
}

newGrapevine :: Maybe String -> Int -> IO Grapevine
newGrapevine name port = do
  let sockAddr = SockAddrInet (fromIntegral port) iNADDR_ANY
  inSock <- socket AF_INET Stream 0
  setSocketOption inSock ReusePort 1
  bind inSock sockAddr
  listen inSock 128
  inPort <- socketPort inSock
  bc <- newBoundedChan 16384
  sc <- newBoundedChan 64
  emptyPeerage <- newMVar M.empty
  noSubjects <- newEmptyMVar
  emptyPres <- newMVar S.empty
  emptySucs <- newMVar M.empty
  emptyNetStats <- newMVar M.empty
  emptySeens <- newMVar $ replicate 3 S.empty
  meshedFalse <- newMVar False
  pure $ Grapevine {
    myNetName = name,
    myPort = inPort,
    mySock = inSock,
    blobCh = bc,
    statsCh = sc,
    peerage = emptyPeerage,
    subjects = noSubjects,
    pres = emptyPres,
    sucs = emptySucs,
    netStats = emptyNetStats,
    seenTables = emptySeens,
    isMeshed = meshedFalse
  }

getPeerage :: Grapevine -> IO (M.Map String SockAddr)
getPeerage = readMVar . peerage

putStats :: Grapevine -> ByteString -> IO ()
putStats gv stats = do
  status <- tryWriteChan (statsCh gv) ("", stats)
  when (not status) $ putStrLn "channel full: STATS DROPPED"

getStats :: Grapevine -> IO (String, ByteString)
getStats = readChan . statsCh

grapevineSize :: Grapevine -> IO Int
grapevineSize gv = M.size <$> readMVar (peerage gv)

grapevinePort :: Grapevine -> PortNumber
grapevinePort = myPort

grapevineKing :: Int -> IO Grapevine
grapevineKing port = do
  emptySubjects <- newMVar M.empty
  gv0 <- newGrapevine Nothing port
  let gv = gv0 { subjects = emptySubjects }
  putStrLn $ "PORT = " ++ show (myPort gv)
  void $ forkIO $ kingLoop gv
  pure gv

-- | Open an outbound socket on the same socket we're listening on.
reuseMyPort :: Grapevine -> IO Socket
reuseMyPort gv = do
  outSock <- socket AF_INET Stream 0
  setSocketOption outSock ReusePort 1
  bind outSock $ SockAddrInet (myPort gv) iNADDR_ANY
  pure outSock

grapevineNoble :: String -> String -> Int -> IO Grapevine
grapevineNoble king name port = do
  let [host, seedPort] = splitOn ":" king
  gv <- newGrapevine (Just name) port
  addrInfo <- getAddrInfo Nothing (Just host) (Just seedPort)
  outSock <- reuseMyPort gv
  connect outSock (addrAddress $ head addrInfo)
  h <- socketToHandle outSock ReadWriteMode
  void $ forkIO $ nobleLoop gv
  void $ forkIO $ handshake gv h
  pure gv

handshake :: Grapevine -> Handle -> IO ()
handshake gv h = do
  -- 1. Say Hello.
  wire h $ pack $ show $ Hello $ fromJust $ myNetName gv
  -- 2. Read Peerage.
  Just (Peerage meshed m) <- readMay . unpack <$> procure h
  let Just ps = readPeerage m
  void $ swapMVar (peerage gv) ps
  print =<< readMVar (peerage gv)
  void $ swapMVar (isMeshed gv) meshed
  socialize gv
  -- 3. Say OK after meshing.
  wire h "OK"
  -- 4. Send stats.
  forever $ wire h . snd =<< readChan (statsCh gv)

nobleLoop :: Grapevine -> IO ()
nobleLoop gv = forever $ do
  (sock, peer) <- accept $ mySock gv
  void $ forkIO $ do
    h <- socketToHandle sock ReadWriteMode
    bs <- procure h
    case readMay $ unpack bs of
      Just (Edict b) -> do  -- Only King sends these.
        process gv b
        yell gv b
      Just (Predecessor s) -> slurp gv s h
      Just (Successor s) -> do
        putStrLn $ "SUCCESSOR RECONNECT: " ++ show peer
        stream gv s h
      _ -> putStrLn "BAD GREETING"
    hClose h

kingLoop :: Grapevine -> IO ()
kingLoop gv = forever $ do
  (sock, peer) <- accept $ mySock gv
  void $ forkIO $ do
    h <- socketToHandle sock ReadWriteMode
    putStrLn $ show peer
    bs <- procure h
    case readMay $ unpack bs of
      Just (Hello s) -> do
        ps <- takeMVar $ peerage gv
        putMVar (peerage gv) $ M.insert s peer ps
        modifyMVar_ (subjects gv) $ pure . M.insert s h
        alreadyMeshed <- readMVar $ isMeshed gv
        when (alreadyMeshed) $ sendAlreadyMeshedPeerage gv h
      _ -> putStrLn "BAD HELLO"

sendAlreadyMeshedPeerage :: Grapevine -> Handle -> IO ()
sendAlreadyMeshedPeerage gv h = do
  ps <- readMVar $ peerage gv
  wire h $ pack $ show $ Peerage True $ M.map show ps
  bs <- procure h
  when (bs /= "OK") $ ioError $ userError "EXPECT OK"

readPeerage :: M.Map String String -> Maybe (M.Map String SockAddr)
readPeerage m = Just $ M.map f m where
  f s = SockAddrInet (read port) host where
    [ip4, port] = splitOn ":" s
    host = toHostAddress $ read ip4

wire :: Handle -> ByteString -> IO ()
wire h s = do
  let n = B.length s
  when (n > 5 * 1024 * 1024) $ ioError $ userError "wire: artifact too large!"
  let ds = map (chr . (`mod` 256) . div n) $ (256^) <$> [3, 2, 1, 0 :: Int]
  forM_ ds $ hPutChar h
  B.hPut h s
  hFlush h

procure :: Handle -> IO ByteString
procure h = do
  ds <- unpack <$> B.hGet h 4
  when (null ds) $ ioError $ userError $ "handle closed"
  let n = sum $ zipWith (*) (ord <$> ds) $ (256^) <$> [3, 2, 1, 0 :: Int]
  when (n > 5 * 1024 * 1024) $ ioError $ userError $ "BUG! Artifact too large: " ++ show n
  r <- B.hGet h n
  when (B.null r) $ ioError $ userError $ "handle closed"
  pure r

-- | Connect to direct successors.
outConnect :: Grapevine -> [(String, SockAddr)] -> IO ()
outConnect gv succs = forM_ succs $ \(s, sock) -> do
  -- It'd be nice to reuse the listen port. Some solutions:
  --
  --  * Wait until everyone has the Peerage first, figure out
  -- the bidirectional connections, then handle them specially.
  --
  --  * Record the handles of incoming connections, catch exceptions when we
  -- connect, and stream to them if the link is meant to be bidirectional.
  --
  --  * Once we receive the Peerage, some scheme determines who initiates a
  -- bidrectional connection. For outgoing connections, this just means we
  -- might also have to slurp the handle. Meanwhile, in the accept loop, we
  -- use MVars so that we stream to existing certain incoming connections but
  -- also set up bidirectional links when handling new ones.
  --
  -- The second solution sounds easiest, though seems a bit unclean.
  tmp <- socket AF_INET Stream 0
  connect tmp sock
  h <- socketToHandle tmp ReadWriteMode
  void $ forkIO $ stream gv s h

-- | If already meshed, tell direct predecessors we're back online.
-- Should be called before outConnect so we can reuse ports.
inConnect :: Grapevine -> [(String, SockAddr)] -> IO ()
inConnect gv preds = (readMVar (isMeshed gv) >>=) $ flip when $
  forM_ preds $ \(s, sock) -> void $ forkIO $ do
    tmp <- reuseMyPort gv
    connect tmp sock
    h <- socketToHandle tmp ReadWriteMode
    wire h $ pack $ show $ Successor $ fromJust $ myNetName gv
    bs <- procure h
    case readMay $ unpack bs of
      Just (Predecessor _) -> slurp gv s h
      _ -> ioError $ userError "WANT REPLY: 'Predecessor'"

socialize :: Grapevine -> IO ()
socialize gv = do
  ps <- readMVar $ peerage gv
  let n = M.size ps
  if n < 10 then do
    let others = filter ((/= fromJust (myNetName gv)) . fst) $ M.assocs ps
    inConnect  gv others
    outConnect gv others
  else kautz gv ps $ kautzRecommend n

kautz :: Grapevine -> M.Map String SockAddr -> (Int, Int) -> IO ()
kautz gv ps (m, n) = let
  sz = kautzSize m n
  lim = M.size ps
  i = M.findIndex (fromJust $ myNetName gv) ps
  in assert (sz >= lim) $ assert (sz <= lim * 2) $ do
    inConnect  gv $ (`M.elemAt` ps) <$> kautzInRoomy  m n i lim
    outConnect gv $ (`M.elemAt` ps) <$> kautzOutRoomy m n i lim

reportSighting :: Ord a => [Set a] -> a -> [Set a]
reportSighting seens@(a:as) h = if S.size a == 1024
  then S.singleton h : init seens
  else S.insert h a : as
reportSighting [] _ = error "BUG!"

-- | Add to a stat counter.
add :: Grapevine -> Int -> String -> IO ()
add gv n s = do
  st <- takeMVar $ netStats gv
  putMVar (netStats gv) $! M.insertWith (+) s n st

-- | Incrmeent a stat counter.
inc :: Grapevine -> String -> IO ()
inc gv s = add gv 1 s

process :: Grapevine -> ByteString -> IO ()
process gv b = do
  seens <- takeMVar $ seenTables gv
  let h = SHA256.hash b
  if any (S.member h) seens then do
    inc gv "dup"
    putMVar (seenTables gv) seens
  else do
    inc gv "in"
    putMVar (seenTables gv) $! reportSighting seens h
    status <- tryWriteChan (blobCh gv) b
    if status then do
      st <- takeMVar $ netStats gv
      let
        st1 = M.insertWith (+) "inqueue" 1 st
        n = st1 M.! "inqueue"
      putMVar (netStats gv) $!
        if n > fromMaybe 0 (M.lookup "inqueue-high-water-mark" st1) then M.insert "inqueue-high-water-mark" n st1 else st1
    else do
      inc gv "indropped"
      putStrLn "FULL BUFFER"

publish :: Grapevine -> IO ()
publish gv = do
  ps <- readMVar $ peerage gv
  ss <- readMVar $ subjects gv
  void $ takeMVar $ isMeshed gv
  doneV <- newEmptyMVar
  let
    waitFor 0 = putMVar (isMeshed gv) True
    waitFor n = do
      takeMVar doneV
      waitFor $ n - 1
  void $ forkIO $ waitFor $ M.size ss
  -- Give every noble the Peerage, then wait for each to connect to their
  -- successors.
  forM_ ss $ \h -> void $ forkIO $ do
    wire h $ pack $ show $ Peerage False $ M.map show ps
    bs <- procure h
    when (bs /= "OK") $ ioError $ userError "EXPECT OK"
    putMVar doneV ()
  -- Only continue once all are ready.
  void $ readMVar $ isMeshed gv
  -- Listen to stats.
  forM_ (M.assocs ss) $ \(s, h) -> void $ forkIO $ do
    let
      discon e = do
        putStrLn $ "DISCONNECT: " ++ s ++ ": " ++ show (e :: SomeException)
        hClose h
    handle discon $ forever $ do
      stats <- procure h
      status <- tryWriteChan (statsCh gv) (show $ ps M.! s, stats)
      when (not status) $ do
        inc gv "statsdrop"
        putStrLn "king: STATS DROPPED"

-- | Slurp messages from a handle.
-- Counterpart to `stream`.
slurp :: Grapevine -> String -> Handle -> IO ()
slurp gv s h = do
  let
    discon e = do
      add gv (-1) ("in/" ++ show s ++ "/connected")
      putStrLn $ "DISCONNECT: " ++ show s ++ ": " ++ show (e :: SomeException)
      modifyMVar_ (pres gv) $ pure . S.delete s
      hClose h
  modifyMVar_ (pres gv) $ pure . S.insert s
  inc gv ("in/" ++ s ++ "/connected")
  handle discon $ forever $ do
    process gv =<< procure h
    inc gv ("in/" ++ s ++ "/msg")

-- | Blast messages to a handle.
-- Counterpart to `slurp`.
-- The Socket Address can be found in the Peerage.
stream :: Grapevine -> String -> Handle -> IO ()
stream gv s h = do
  ch <- newBoundedChan 128
  modifyMVar_ (sucs gv) $ pure . M.insert s (ch, h)
  wire h $ pack $ show (Predecessor $ fromJust $ myNetName gv)
  let
    discon e = do
      putStrLn $ "DISCONNECT: " ++ s ++ ": " ++ show (e :: SomeException)
      modifyMVar_ (sucs gv) $ pure . M.delete s
      hClose h
  handle discon $ forever $ do
    b <- readChan ch
    {-
    t <- getTime Realtime
    let
      s = show $ B.length b
      msg = concat [show $ sec t, ".", printf "%06d" $ nsec t `div` 1000, ": ", s]
    putStrLn msg
    -}
    wire h b

yell :: Grapevine -> ByteString -> IO ()
yell gv b = if isNothing $ myNetName gv then do
    ps <- readMVar $ peerage gv
    tmp <- socket AF_INET Stream 0
    connect tmp $ snd $ M.findMin ps
    h <- socketToHandle tmp WriteMode
    wire h $ pack $ show $ Edict b
    hClose h
  else do
    inc gv "out"
    ns <- readMVar $ sucs gv
    seens <- takeMVar $ seenTables gv
    forM_ (M.assocs ns) $ \(s, (ch, _)) -> do
      roomy <- tryWriteChan ch b
      when (not roomy) $ putStrLn $ "OUT CHANNEL FULL: " ++ s
      BChan.writeChan ch b
    putMVar (seenTables gv) $! reportSighting seens $ SHA256.hash b

hear :: Grapevine -> IO ByteString
hear gv = do
  r <- readChan $ blobCh gv
  add gv (-1) "inqueue"
  pure r

htmlNetStats :: Grapevine -> IO String
htmlNetStats gv = do
  ns <- readMVar $ sucs gv
  ps <- readMVar $ peerage gv
  t <- readMVar $ netStats gv
  pure $ concat
    [ "successors:\n"
    , (unlines $ map show $ catMaybes $ (`M.lookup` ps) <$> M.keys ns) ++ "\n"
    , unlines $ show <$> M.assocs t
    ]

grapevineTable :: Grapevine -> IO (M.Map String Int)
grapevineTable gv = readMVar $ netStats gv
