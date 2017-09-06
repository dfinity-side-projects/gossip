import Control.Monad
import Grapevine

main :: IO ()
main = do
  gv <- grapevineKing "CENTRAL" 4000
  void $ getLine
  putStrLn "PUBLISH!"
  publish gv
