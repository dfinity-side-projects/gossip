import Data.ByteString.Char8 (pack)
import Control.Monad
import Grapevine

main :: IO ()
main = do
  gv <- grapevineKing "CENTRAL" 4000
  putStrLn "Press Enter to publish peerage."
  void $ getLine
  publish gv
  yell gv $ pack "GO"
