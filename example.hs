{-# LANGUAGE OverloadedStrings #-}
import Control.Concurrent
import Control.Concurrent.Async
import Control.Monad
import Data.ByteString.Char8 (pack)
import Data.List
import Grapevine

main :: IO ()
main = do
  let
    sz = 32
    names = show <$> [1..sz]
  central <- grapevineKing "CENTRAL" 0
  gvs <- forConcurrently names $ \name -> grapevineNoble ("localhost:" ++ show (grapevinePort central)) name 0
  let
    waitForSz = do
      n <- grapevineSize central
      when (n < sz) $ threadDelay 10000 >> waitForSz
  waitForSz
  publish central
  yell central $ pack "READY"
  forConcurrently_ (zip gvs names) $ \(gv, name) -> do
    ok <- hear gv
    when (ok /= "READY") $ putStrLn "EXPECT READY"
    let
      m = pack name
      f ms = if length ms == sz then
          if sort ms /= sort (pack <$> names) then
            putStrLn $ concat ["want ", show names, " got ", show ms]
          else
            putStrLn "PASSED"
        else do
          b <- hear gv
          yell gv b
          f (b:ms)
    yell gv m
    f [m]
