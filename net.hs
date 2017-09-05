{-# LANGUAGE OverloadedStrings #-}
import Data.ByteString.Char8 (ByteString, pack, unpack)
import Control.Concurrent
import Control.Monad
import Data.List.Split
import Data.Maybe
import qualified Data.Map.Strict as M
import Network.Socket hiding (send, recv)
import Network.Socket.ByteString
import System.Environment
import Text.Read
 
data Message = Hello String | Peerage (M.Map String String) | Blob ByteString deriving (Show, Read)

type Grapevine = MVar (M.Map String SockAddr)

newGrapevine :: IO Grapevine
newGrapevine = newMVar M.empty

main :: IO ()
main = do
  envs <- getEnvironment
  let
    mport = readMaybe =<< lookup "PORT" envs
    port = fromMaybe 0 mport
    sockAddr = SockAddrInet port iNADDR_ANY
  inSock <- socket AF_INET Stream 0
  setSocketOption inSock ReusePort 1
  bind inSock sockAddr
  listen inSock 2
  inPort <- socketPort inSock
  putStrLn $ "PORT = " ++ show inPort

  outSock <- socket AF_INET Stream 0
  setSocketOption outSock ReusePort 1
  bind outSock $ SockAddrInet inPort iNADDR_ANY

  gv <- newGrapevine

  args <- getArgs
  if null args then do  -- King.
    let
      dumpLoop = do
        publish gv
        threadDelay $ 30 * 1000000
        dumpLoop

    void $ forkIO $ dumpLoop
    kingLoop gv inSock

  else do  -- Noble.
    let
      [host, port] = splitOn ":" $ head args
      name = fromMaybe "foo" $ lookup "NAME" envs
    addrInfo <- getAddrInfo Nothing (Just host) (Just port)
    connect outSock (addrAddress $ head addrInfo)
    send outSock $ pack $ show $ Hello name
    bs <- recv outSock 4096
    print ("GOT", bs)
    nobleLoop gv inSock

kingLoop :: Grapevine -> Socket -> IO ()
kingLoop gv listenSock = do
  (sock, peer) <- accept listenSock
  void $ forkIO $ do
    putStrLn $ show peer
    bs <- recv sock 4096
    case readMaybe $ unpack bs of
      Nothing -> send sock "ERROR"
      Just (Hello s) -> do
        pm <- takeMVar gv
        putMVar gv $ M.insert s peer pm
        send sock $ pack "OK"
    close sock
  kingLoop gv listenSock

nobleLoop :: Grapevine -> Socket -> IO ()
nobleLoop gv listenSock = do
  (sock, peer) <- accept listenSock
  void $ forkIO $ do
    putStrLn $ show peer
    bs <- recv sock 4096
    case readMaybe $ unpack bs of
      Nothing -> send sock "ERROR"
      Just (Hello s) -> do
        pm <- takeMVar gv
        putMVar gv $ M.insert s peer pm
        send sock $ pack "OK"
    close sock
  nobleLoop gv listenSock

publish :: Grapevine -> IO ()
publish gv = do
  -- Send Peerage to all peers
  ps <- readMVar gv
  print ps

cast :: Grapevine -> ByteString -> IO ()
cast gv msg = do
  --
  undefined
