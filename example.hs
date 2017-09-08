import Control.Concurrent.Async
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
  publish central
  forConcurrently_ (zip gvs names) $ \(gv, name) -> do
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
