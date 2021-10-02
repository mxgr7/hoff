{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# OPTIONS_GHC -Wno-unused-imports #-}

module Hoff.Examples where 

import qualified Chronos as C
import           Control.Monad
import           Data.Functor.Compose
import           Data.Record.Anon.Advanced (Record)
import qualified Data.Vector as V
import           Hoff as H
import           Hoff.HSql.Operators as H
import           Hoff.Utils
import qualified Prelude
import           System.Environment
import           System.FilePath
import           Text.InterpolatedString.Perl6
import           Yahp hiding (take, group, (^), sum, lookup, error)


error :: ConvertText a [Char] => a -> c
error = Prelude.error . toS

main :: IO ()
main = do
  print t1
  print t2
  print t4
  ex3
  print sa1
  print m1
  -- mapM putStrLn [show te7, show te4, show te5, show te6, show te8]
  example1
  example2
  print 'a'
  example3
  print 'b'
  example4
  print 'c'
  example5
  print 'd'
  example6
  example8
  example7

t1 = unsafeH $ #a <# [9,7,7,3 :: Double] // tc #b [10,20,40,30 :: Int64] // tc #c ['a','a','b','b'] // tc #d ["a","a","b","b"::Text]

-- t3 = select [ei @Int #b #a, #c </ liftV2 ((+) @Int) #a #b, #k </ fmap (C.Time . fromIntegral @Int64) #b ] t1

t2 = unsafeH $ select [#b </ mapE (id @Int64) #b,  #k </ mapE (\x -> C.Time $ fromIntegral (x :: Int64)) #b ] t1



sum :: Num b => Exp b -> Agg b
sum = mapA V.sum

t4      = unsafeH $ selectBy [#s </ sum @Double #a] [sn #k #c]                   t1
t4'     = unsafeH $ selectBy [ai $ sum @Double #a] #c                   t1
t4''    = unsafeH $ updateBy [ei #asd $ sum @Double #a, ai $ sum @Double #a] #c                   t1
f1      = unsafeH $ update [#ab </ fby (sum @Double #a) #b, #ac </ fby (sum @Double #a) #c]  t1


t5e= t4 !!?/ (tc' #k ["a"::String])
t5 = t4 !!?/ (tc' #k ['a'])

t6 = t4 !!?/ (unsafeH $ #a <# [1::Int,3, 5] // #k <# ['a','c','b'])

t7 = lj (unsafeH $ #a <# [9,7::Int,7] // #c <# V.fromList "abc") $ xkey ["a","c"] $ unsafeH $ update [ai $ floor_ @Double @Int #a] t1

s = #k <# [1::Int,2]  // #a <# ("ab"::String) // #c <# [1,2::Int] // tcF #d [Nothing, Just (1::Int)]
s2 = #k <# [1::Int,2]  // #a <# ("AB"::String) // #c <# [1,2::Int] // tcF #d [Nothing, Just (1::Int)]

t = #k <# [1::Int,2,3]  // #b <# ("XYZ"::String) // #c <# [10,20,30::Int] //  #d <# [10,20,30::Int]

j1 = lj t $ xkey [#k] s
j2 = lj s $ xkey [#k] t


k1 = xkey ["a","c"] $ update [ai $ floor_ @Double @Int #a] t1


t9 = tcF #a [Just 9,Nothing :: Maybe Int] // tc #b [10,20 :: Int]
  // #b <# [Just True,Nothing] 
  // #s <# [Nothing, Just 'a'] 

example1 :: IO ByteString
example1 = do
  pbin <- getEnv "PYTHON_BIN"
  pythonEvalByteString pbin script ["arg"] Nothing $ Just "\DLE\NUL"
  where script :: Text
        script = [q|
data = sys.stdin.buffer.read()
print(sys.argv[2])
with open(sys.argv[1], 'wb') as f:
  f.write(('stdin: ' + str(data) + ', raw: ').encode())
  f.write(data)
|]
        
ex2 = do
  pbin <- getEnv "PYTHON_BIN"
  print ta
  either error (putStrLn . show @Table) =<< pythonEvalHoff pbin ("outputDf = inputDf"::Text) [] Nothing (Just ta)

example2 :: IO ()
example2 = do
  pbin <- getEnv "PYTHON_BIN"
  print ta
  print $ meta ta
  t <- either error pure =<< pythonEvalHoff pbin script ["arg"] Nothing (Just ta)
  print t
  print $ meta t
  where script :: Text
        script = [q|
import pandas as pd
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 80)
print(inputDf)
print(inputDf.dtypes)
print("sys.argv", sys.argv, sys.argv[1])
outputDf = inputDf
|]

example3 :: IO Table
example3 = do
  pbin <- getEnv "PYTHON_BIN"
  t <- either error pure =<< pythonEvalHoff pbin script ["arg"] Nothing (Just t2)
  t <$ print (meta t)
  where script :: Text
        script = [q|
import pandas as pd
print("sys.argv", sys.argv, sys.argv[1])
outputDf = pd.DataFrame({'IntCol': [1,10], 'BoolCol':[True,False], 'TextCol':['sd','yi'], 'DoubleCol':[1.3, None]})
|]

ta = unsafeH $ tc #a [9,7,3 :: Double]
  // tc #b [10,20,2 :: Int64]
  // tc #c ['µ','a','b']
  // tc #d ["a","µ","b"::Text]
  // tc #e [True, False, True]
  // tc #f (C.Day <$> [10,20,2])
  // tc #g (C.Time . (* 10000000000) <$> [1,3,4])
  // tcF #am (toM [9,7,3 :: Double])
  // tcF #bm (toM [10,20,2 :: Int64])
  // tcF #cm (toM ['µ','a','b'])
  // tcF #dm (toM ["a","µ","b"::Text])
  // tcF #em (toM [True, False, True])
  // tcF #fm (toM (C.Day <$> [10,20,2]))
  // tcF #gm (toM (C.Time . (* 1000000000) <$> [1,3,4]))
  where toM [a,_,c]=[Just a, Nothing, Just c]
        toM _ = Prelude.error "asd"
        toM :: [a] -> [(Maybe a)]

tw = selectW [] (mapE (=='a') #c) ta
tdel2 = H.deleteW (mapE (=='a') #c) ta
tu = updateW [#a </ co @Double 100] (mapE (=='a') #c) ta
tu3 = updateW [ai $ co @Double 100] (mapE (=='a') #c) ta
tu2 = updateBy [ai $ sum @Double #a] [#e] ta

td = H.delete [#a] ta

te      = print $ execC ANON_F { b = #b :: Exp Int64 } ta
te2     = print $ flipRecord <$> execC ANON_F { b = #b :: Exp Int64 } ta
te3     = unsafeH $ execBy (sum #b) #c ta :: VectorDict Char Int64
te7     = unsafeH $ execBy (Just <$> sum #b) #e ta :: VectorDict Bool (Maybe Int64)
te4     = unsafeH $ execRBy ANON_F { a = sum @Int64 #b, b = mapA (count @(Vector Int64)) #b } (#c :: Exp Char) ta
te5     = unsafeH $ execRBy ANON_F { a = sum @Int64 #b, b = countA } (co 'a') ta
te6     = unsafeH $ execBy (liftA2 (,) (sum @Int64 #b) $ mapA @Int64 count #b) (co 'a') ta
te8     = unsafeH $ execRBy ANON_F { a = sum @Int64 #b, b = countDistinct #e, c = countDistinct #c } (co 'a') ta

idn = #a <# [None ()]

example4 :: IO Table
example4 = do
  pbin <- getEnv "PYTHON_BIN"
  t <- either error pure =<< pythonEvalHoff pbin script ["arg"] Nothing (Just t2)
  print (meta t)
  print t
  let t2 = unsafeH $ select [af @Maybe @Int64 "IntCol"] t
  print (meta t2)
  return t2
  where script :: Text
        script = [q|
import pandas as pd
print("sys.argv", sys.argv, sys.argv[1])
outputDf = pd.DataFrame({'IntCol': [None]})
|]


d1 = unsafeH $ dictV [10,2::Int64,3] ("asd" :: String)
td1 = unsafeH $ update [em #lookup $ d1 ?. #b] ta
td2 = unsafeH $ update [ei #lookup $ d1 !. #b] ta

sa1 = unsafeH $ selectAgg [ai $ sum @Double #a, ai countA, ai $ countDistinct #e, ai $ countDistinct #a] ta

ex3 = do
  pbin <- getEnv "PYTHON_BIN"
  pythonEvalHoff pbin ("outputDf = pd.DataFrame({'a': [1.2, None], 'b':['asd','a']})"::Text) [] Nothing Nothing

eth1 = partitionEithers_ (Proxy @()) #a $ update [af $ fmap (bool (Left ()) (Right True) . (> (4::Double))) #a] ta
eth2 = lefts_ (Proxy @()) #a $ update [af $ fmap (bool (Left ()) (Right True) . (> (4::Double))) #a] ta
m1 = partitionMaybes #a $ ta


example5 :: IO Table
example5 = do
  pbin <- getEnv "PYTHON_BIN"
  t <- either error pure =<< pythonEvalHoff pbin script ["arg"] Nothing (Just t2)
  print (meta t)
  print t
  -- let t2 = select [af @Maybe @Int64 "IntCol"] t
  -- print (meta t2)
  return t2
  where script :: Text
        script = [q|
import pandas as pd
print("sys.argv", sys.argv, sys.argv[1])
outputDf = pd.DataFrame({'Asd': [None, 2.3, 2.2]})
|]


example6 :: IO ()
example6 = do
  pbin <- getEnv "PYTHON_BIN"
  (n,t) <- either error (pure . unzip) =<< pythonEvalHoffs pbin script ["arg"] Nothing (Just t2)
  print n
  mapM_ (print . meta) t
  where script :: Text
        script = [q|
import pandas as pd
print("sys.argv", sys.argv, sys.argv[1])
outputDfs = [   ('table1', pd.DataFrame({'Asd': [None, 2.3, 2.2]})),
                ('table2', pd.DataFrame({'second table': [None, 2.3, 2.2]}))]
|]


example7 :: IO ()
example7 = do
  pbin <- getEnv "PYTHON_BIN"
  (n,t) <- either error (pure . unzip) =<< pythonEvalHoffs pbin script ["arg"] Nothing (Just t2)
  print n
  mapM_ (\x -> print (meta x) >> print x) t
  where script :: Text
        script = [q|
import pandas as pd
print("sys.argv", sys.argv, sys.argv[1])
outputDfs = [   ('table1', pd.DataFrame({'Asd': []})),
                ('table2', pd.DataFrame({'second table': [None, 2.3, 2.2]}))]
|]


example8 :: IO ()
example8 = do
  pbin <- getEnv "PYTHON_BIN"
  (n,t) <- either error (pure . unzip) =<< pythonEvalHoffs pbin script ["arg"] Nothing (Just t2)
  print n
  mapM_ (\x -> print (meta x) >> print x) t
  where script :: Text
        script = [q|
import pandas as pd
import numpy as np
print("sys.argv", sys.argv, sys.argv[1])
outputDfs = [('a',pd.DataFrame({'a':[np.NaN,None]}, dtype='str'))]
|]


ex9 = typedTable @'["a" := I Double] $ execC ANON_F { a = #a :: Exp (I Double) } t1


asd = summary $ #a <# [None (), None (), None (), None ()] //
      #b <# ("abbc"::String) //
      tcF #c [Nothing, Just 1, Just 3, Just @Int 2]


asd2 = selectW [] ((== 'a') <$> #asc) t1

asd3 = putStrLn $ unlines $ toList $ showSingleLine @Text  <$> unsafeH (rows t1)

fr1 = fromRowsI ANON_F { a = fst, b = snd } $ toVector [(1::Int,2::Word), (10,20)]
