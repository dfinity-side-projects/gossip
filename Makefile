GHC=ghc -O -threaded -Wall
all: ; $(GHC) demo.hs && $(GHC) king.hs
test: ; $(GHC) Test.hs && ./Test
ex: ; $(GHC) example.hs
