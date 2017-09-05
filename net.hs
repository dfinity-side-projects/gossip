{-# LANGUAGE OverloadedStrings #-}
import Data.ByteString.Char8 (ByteString, pack, unpack)
import Control.Concurrent hiding (readChan)
import Control.Concurrent.BoundedChan
import Control.Monad
import Data.IP
import Data.List.Split
import Data.Maybe
import qualified Data.Map.Strict as M
import Network.Socket hiding (send, recv)
import Network.Socket.ByteString
import System.Environment
import Text.Read
 
data Message = TestGo | Hello String | Peerage (M.Map String String) | Blob ByteString deriving (Show, Read)

data Grapevine = Grapevine {
  myNetName :: String,
  myPort :: PortNumber,
  blobChan :: BoundedChan ByteString,
  peerage :: M.Map String SockAddr
}

reuseMyPort :: Grapevine -> IO Socket
reuseMyPort gv = do
  outSock <- socket AF_INET Stream 0
  setSocketOption outSock ReusePort 1
  bind outSock $ SockAddrInet (myPort gv) iNADDR_ANY
  pure outSock

main :: IO ()
main = do
  envs <- getEnvironment
  let
    mport = readMaybe =<< lookup "PORT" envs
    port = fromMaybe 0 mport
    sockAddr = SockAddrInet port iNADDR_ANY
    name = fromMaybe "foo" $ lookup "NAME" envs
  inSock <- socket AF_INET Stream 0
  setSocketOption inSock ReusePort 1
  bind inSock sockAddr
  listen inSock 2
  inPort <- socketPort inSock
  putStrLn $ "PORT = " ++ show inPort
  bc <- newBoundedChan 20
  gv <- newMVar $ Grapevine name inPort bc M.empty

  args <- getArgs
  if null args then kingLoop gv inSock else do
    let
      [host, port] = splitOn ":" $ head args
    addrInfo <- getAddrInfo Nothing (Just host) (Just port)
    outSock <- reuseMyPort =<< readMVar gv
    connect outSock (addrAddress $ head addrInfo)
    send outSock $ pack $ show $ Hello name
    bs <- recv outSock 4096
    print ("GOT", bs)
    close outSock
    nobleLoop gv inSock

kingLoop :: MVar Grapevine -> Socket -> IO ()
kingLoop gv listenSock = do
  (sock, peer) <- accept listenSock
  void $ forkIO $ do
    putStrLn $ show peer
    bs <- recv sock 4096
    case readMaybe $ unpack bs of
      Just TestGo -> do
        publish gv
      Just (Hello s) -> do
        g <- takeMVar gv
        putMVar gv $ g { peerage = M.insert s peer $ peerage g }
        void $ send sock "OK"
      _ -> void $ send sock "ERROR"
    close sock
  kingLoop gv listenSock

readPeerage :: M.Map String String -> Maybe (M.Map String SockAddr)
readPeerage m = Just $ M.map f m where
  f s = SockAddrInet (read port) host where
    [ip4, port] = splitOn ":" s
    host = toHostAddress $ read ip4

nobleLoop :: MVar Grapevine -> Socket -> IO ()
nobleLoop gv listenSock = do
  (sock, peer) <- accept listenSock
  void $ forkIO $ do
    bs <- recv sock 4096
    case readMaybe $ unpack bs of
      Just (Peerage m) -> do
        case readPeerage m of
          Nothing -> void $ send sock "ERROR"
          Just ps -> do
            g <- takeMVar gv
            putMVar gv $ g { peerage = ps }
            {-
            forM (M.elems ps) $ \s -> do
              tmpSock <- socket AF_INET Stream 0
              connect tmpSock s
              send tmpSock $ pack "HI!"
              bs <- recv tmpSock 4096
              print ("RCPT", bs)
              -}
            void $ send sock "OK"
      Just TestGo -> do
        cast gv "TEST BLOB"
      Just (Blob b) -> do
        print ("BLOB", b)
        bc <- blobChan <$> readMVar gv
        status <- tryWriteChan bc b
        void $ send sock $ if status then "OK" else "FULL"
      _ -> void $ send sock "ERROR"
    close sock
  nobleLoop gv listenSock

publish :: MVar Grapevine -> IO ()
publish gv = do
  -- Send Peerage to all peers
  g <- readMVar gv
  let ps = peerage g
  forM_ (M.elems ps) $ \sock -> do
    tmpSock <- socket AF_INET Stream 0
    connect tmpSock sock
    send tmpSock $ pack $ show $ Peerage $ M.map show ps 
    bs <- recv tmpSock 4096
    print ("RCPT", bs)
  print ps

cast :: MVar Grapevine -> ByteString -> IO ()
cast gv b = do
  g <- readMVar gv
  forM_ (M.assocs $ peerage g) $ \(s, sock) -> when (s /= myNetName g) $ do
    tmpSock <- socket AF_INET Stream 0
    connect tmpSock sock
    send tmpSock $ pack $ show $ Blob b
    bs <- recv tmpSock 4096
    print ("RCPT", bs)

overhear :: MVar Grapevine -> IO ByteString
overhear gv = do
  bc <- blobChan <$> readMVar gv
  readChan bc
