{-# LANGUAGE OverloadedStrings #-}
import Blaze.ByteString.Builder (fromByteString)
import Control.Concurrent
import Control.Monad
import Data.ByteString.Char8 (ByteString, pack, unpack)
import Data.ByteString.Lazy (toStrict)
import Network.Wai
import Network.Wai.EventSource
import Network.Wai.Handler.Warp (runEnv)
import Network.HTTP.Types
import System.Entropy
import Text.Printf

import Grapevine

main :: IO ()
main = do
  sseCh <- newChan
  seed <- getEntropy 32
  let name = concatMap (printf "%02X") $ unpack seed
  gv <- grapevineNoble "localhost:4000" name 0
  void $ forkIO $ forever $ do
    s <- hear gv
    yell gv s
    writeChan sseCh $ ServerEvent Nothing Nothing [fromByteString s]
  user <- (concatMap (printf "%02x") . unpack) <$> getEntropy 16
  nonce <- newMVar (0 :: Int)
  runEnv 3000 $ \req f -> let
    redir = redirTo ""
    redirTo s = f $ responseBuilder status303 [(hLocation, mconcat ["/", s])] $
      fromByteString $ mconcat ["Redirecting to /", s, "\n"]
    in  case requestMethod req of
    "GET" -> case pathInfo req of
      [] -> do
        putStrLn $ "GET: " ++ show (remoteHost req) ++ " " ++ show (rawPathInfo req)
        f $ responseBuilder status200 [] $ fromByteString $ html
      ["sse"] -> eventSourceAppChan sseCh req f
      _ -> redir
    "POST" -> do
      qs <- parseQuery . toStrict <$> strictRequestBody req
      case lookup "chat" qs of
        Just (Just s) -> do
          n <- takeMVar nonce
          putMVar nonce $ n + 1
          let msg = pack $ user ++ " " ++ show n ++ ": " ++ unpack s
          yell gv msg
          writeChan sseCh $ ServerEvent Nothing Nothing [fromByteString msg]
        _ -> pure ()
      redir
    _ -> f $ responseLBS status501 [] "bad method\n"

html :: ByteString
html = mconcat [
  "<html><head><title>Chirp</title></head><body>",
  "<textarea id='log' readonly rows='25' cols='80'></textarea>",
  "<script type='text/javascript'>",
  "var source = new EventSource('/sse');",
  "document.getElementById('log').innerHTML = sessionStorage.getItem('log');",
  "source.onmessage = function(event) {",
  "document.getElementById('log').innerHTML += event.data + '\\n';",
  "sessionStorage.setItem('log', document.getElementById('log').innerHTML);",
  "}",
  "</script>",
  "<form method='POST'><input type='text' size='80' name='chat' autofocus>",
  "<input type='submit' value='Chirp'></form>",
  "</body></html>"]
