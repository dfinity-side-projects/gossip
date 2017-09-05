{-# LANGUAGE OverloadedStrings #-}
module Grapevine (Grapevine, grapevineKing, grapevineNoble, publish, yell, hear) where

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
  mySock :: Socket,
  blobChan :: BoundedChan ByteString,
  peerage :: MVar (M.Map String SockAddr)
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
  listen inSock 2
  inPort <- socketPort inSock
  bc <- newBoundedChan 20
  Grapevine name inPort inSock bc <$> newMVar M.empty

grapevineKing :: String -> Int -> IO Grapevine
grapevineKing name port = do
  let
    sockAddr = SockAddrInet (fromIntegral port) iNADDR_ANY
  gv <- newGrapevine name port
  putStrLn $ "PORT = " ++ show (myPort gv)
  kingLoop gv
  pure gv

grapevineNoble :: String -> String -> Int -> IO Grapevine
grapevineNoble king name port = do
  let
    [host, seedPort] = splitOn ":" king
  gv <- newGrapevine name port
  addrInfo <- getAddrInfo Nothing (Just host) (Just seedPort)
  outSock <- reuseMyPort gv
  connect outSock (addrAddress $ head addrInfo)
  send outSock $ pack $ show $ Hello name
  bs <- recv outSock 4096
  print ("GOT", bs)
  close outSock
  nobleLoop gv
  pure gv
  
main :: IO ()
main = do
  envs <- getEnvironment
  let
    port = fromMaybe 0 $ readMaybe =<< lookup "PORT" envs
    name = fromMaybe "foo" $ lookup "NAME" envs
  args <- getArgs
  void $ if null args then
    grapevineKing name port
  else
    grapevineNoble (head args) name port

kingLoop :: Grapevine -> IO ()
kingLoop gv = do
  (sock, peer) <- accept $ mySock gv
  void $ forkIO $ do
    putStrLn $ show peer
    bs <- recv sock 4096
    case readMaybe $ unpack bs of
      Just TestGo -> do
        publish gv
      Just (Hello s) -> do
        -- TODO: Use our IP if address is localhost.
        ps <- takeMVar $ peerage gv
        putMVar (peerage gv) $ M.insert s peer ps
        void $ send sock "OK"
      _ -> void $ send sock "ERROR"
    close sock
  kingLoop gv

readPeerage :: M.Map String String -> Maybe (M.Map String SockAddr)
readPeerage m = Just $ M.map f m where
  f s = SockAddrInet (read port) host where
    [ip4, port] = splitOn ":" s
    host = toHostAddress $ read ip4

nobleLoop :: Grapevine -> IO ()
nobleLoop gv = do
  (sock, peer) <- accept $ mySock gv
  void $ forkIO $ do
    bs <- recv sock 4096
    case readMaybe $ unpack bs of
      Just (Peerage m) -> do
        case readPeerage m of
          Nothing -> void $ send sock "ERROR"
          Just ps -> do
            void $ swapMVar (peerage gv) ps
            void $ send sock "OK"
      Just TestGo -> do
        yell gv "TEST BLOB"
      Just (Blob b) -> do
        print ("BLOB", b)
        status <- tryWriteChan (blobChan gv) b
        void $ send sock $ if status then "OK" else "FULL"
      _ -> void $ send sock "ERROR"
    close sock
  nobleLoop gv

publish :: Grapevine -> IO ()
publish gv = do
  ps <- readMVar $ peerage gv
  forM_ (M.elems ps) $ \sock -> do
    tmpSock <- socket AF_INET Stream 0
    connect tmpSock sock
    send tmpSock $ pack $ show $ Peerage $ M.map show ps 
    bs <- recv tmpSock 4096
    print ("RCPT", bs)
  print ps

yell :: Grapevine -> ByteString -> IO ()
yell gv b = do
  ps <- readMVar $ peerage gv
  forM_ (M.assocs ps) $ \(s, sock) -> when (s /= myNetName gv) $ do
    tmpSock <- socket AF_INET Stream 0
    connect tmpSock sock
    send tmpSock $ pack $ show $ Blob b
    bs <- recv tmpSock 4096
    print ("RCPT", bs)

hear :: Grapevine -> IO ByteString
hear gv = do
  readChan $ blobChan gv
