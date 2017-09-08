import Data.List
import Test.Tasty
import Test.Tasty.HUnit

import Kautz

assertInverse :: (Int, Int) -> Assertion
assertInverse (m, n) = s @=? (fromKautz m . toKautz m n <$> s)
  where s = [0..kautzSize m n - 1]

main :: IO ()
main = defaultMain $ testGroup "props" $
  [ testCase "K 2 2: 3 hops from 0" $ [0..11] @=?
    sort (nub $ concat $ take 4 $ iterate (concatMap $ kautzOut 2 2) [0])
  , testCase "K 2 2: 3 hops to 0"   $ [0..11] @=?
    sort (nub $ concat $ take 4 $ iterate (concatMap $ kautzIn  2 2) [0])
  ] ++
  (testCase "fromKautz . toKautz" . assertInverse <$> [(m, n) | m <- [2..5], n <- [2..5]])
 
