{-# LANGUAGE QuasiQuotes #-}

module Hoff.JSON where

import           Control.DeepSeq
import           Control.Lens
import           Data.Aeson as A
import           Data.Aeson.Internal as A
import           Data.Aeson.Key as A (toString, toText)
import           Data.Aeson.KeyMap as A (toAscList)
import           Data.Aeson.Parser (eitherDecodeWith)
import           Data.Aeson.Types as A
import qualified Data.List as L
import           Data.SOP
import           Data.Scientific
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as M
import           Hoff.Dict
import           Hoff.H
import           Hoff.Table
import           Hoff.Utils
import           System.IO.Unsafe
import           Text.InterpolatedString.Perl6
import           Yahp as Y

newtype VectorOfRecords = VectorOfRecords       { fromVectorOfRecords   :: Maybe Table }
newtype KeyToRecordMap = KeyToRecordMap         { fromKeyToRecordMap    :: Maybe (SingleKeyTable Text) }

instance FromJSON KeyToRecordMap where parseJSON = fmap KeyToRecordMap . parseKeyToRecordMap []

instance FromJSON VectorOfRecords where parseJSON = fmap VectorOfRecords . parseVectorOfRecords []

parseKeyToRecordMap :: [Text] -> Value -> Parser (Maybe (SingleKeyTable Text))
parseKeyToRecordMap nulls = withObject "Hoff.Table via KeyToRecordMap" g
    where g v = unsafePerformIO $ catch (seq res $ pure2 res) $ pure . fail . fromTableJSONError 
            where res = keyToRecordMap nulls v

parseVectorOfRecords :: [Text] -> Value -> Parser (Maybe Table)
parseVectorOfRecords nulls = withArray "Hoff.Table via VectorOfRecords" g
    where g v = unsafePerformIO $ catch (seq res $ pure2 res) $ pure . fail . fromTableJSONError 
            where res = parseRecords nulls v V.head V.iforM_

parseRecords :: Foldable a => [Text] -> a Value -> (a Value -> Value) ->
  (forall m . Monad m => a Value -> (Int -> Value -> m ())-> m ()) -> Maybe Table
parseRecords nulls v head iterate = seqElements res $ runHMaybe $ tableNoLengthCheck $ dictNoLengthCheck colNames $ res
  where (colNames, colVectors) = fromRecords v head iterate
        res = V.zipWith (vectorColumn $ V.fromList nulls) colNames colVectors

seqElements :: Vector a -> b -> b
seqElements = seq . liftRnf (\x -> seq x ())

keyToRecordMap :: [Text] -> Object -> (Maybe (SingleKeyTable Text))
keyToRecordMap nulls o = ffor tableM $ \table -> dictNoLengthCheck (V.fromList $ A.toText <$> keys) table
  where tableM = parseRecords nulls records L.head $ \v f -> zipWithM_ f [0..] v
        (keys, records) = unzip $ A.toAscList o

-- -- | throws TableJSONError
fromRecords :: Foldable a => a Value -> (a Value -> Value) ->
  (forall m . Monad m => a Value -> (Int -> Value -> m ())-> m ()) -> (Symbols, Vector (Vector Value))
fromRecords vals head iterate = (V.fromList $ toText <$> colNames,) $ runST $ do
  cols <- V.replicateM ncols (M.unsafeNew nrows)
  let g r m = k (A.toAscList m) colNames 0
        where k [] []                      _ = pass
              k ((rc,x):xs) (cname:cnames) c | rc == cname = M.unsafeWrite (V.unsafeIndex cols c) r x >> k xs cnames (succ c)
                                             | True        = throw $ TableJSONError
                                                            $ "Row " <> show (succ r) <> " has an unexpected column " <> toString rc <> " expected " <> toString cname
              k _ _                        _ = throw $ TableJSONError
                                              $ "Row " <> show (succ r) <> " has " <> show (length m) <> " columns. Expected: " <> show ncols
  iterate vals $ \r -> g r . expectObject r
  mapM V.unsafeFreeze cols
  where ncols = length firstRow
        firstRow = expectObject 0 $ head vals
        colNames = fmap fst $ A.toAscList firstRow :: [Key]
        nrows = length vals

wrap :: Wrappable a => (forall s . Int -> Value -> ST s (Maybe a)) -> Vector Value -> TableCol 
wrap f v = toWrappedDyn $ runST $ V.imapM f v

-- -- | throws TableJSONError
vectorColumn :: Vector Text -> Text -> Vector Value -> TableCol
vectorColumn nulls colname v = ($v) $ case firstNonNull of
  String _ -> wrap $ \i -> \case { String t -> pure2 t;                          Null -> pure Nothing; v -> err i v}
  Number _ -> wrap $ \i -> \case { Number t -> pure2 $ toRealFloat @Double t;    Null -> pure Nothing; v -> nullsOrError i v}
  Bool   _ -> wrap $ \i -> \case { Bool t   -> pure2 t;                          Null -> pure Nothing; v -> nullsOrError i v}
  Null     -> \v -> toWrappedDyn $ V.replicate (length v) $ I $ None ()
  v        -> notSup v
  where firstNonNull = fromMaybe Null $ flip V.find v $ \case  
          String s -> not $ V.elem s nulls
          Number _ -> True
          Bool _ ->   True
          Null     -> False
          v        -> notSup v
        nullsOrError :: Applicative f => Int -> Value -> f (Maybe b)
        nullsOrError r = \case
          String v      | V.elem v nulls -> pure Nothing
          v             -> err r v
        err r v = throw $ TableJSONError $ context <> ", Row " <> show (succ r) <> ": expected " <> aesonTypeOf firstNonNull <> " got " <> show v
        err :: Int -> Value -> b
        context = "Column " <> toS colname
        notSup v = throw $ TableJSONError $ context <> ": 'FromJSON Hoff.Table' does not support " <> aesonTypeOf v
        notSup :: Value -> a
        

expectObject :: Int -> Value -> Object
expectObject r = \case
  Object o -> o
  v        -> throw $ TableJSONError $ "Row " <> show (succ r) <> " is not an object: " ++ show v

data TableJSONError = TableJSONError { fromTableJSONError :: String }
  deriving (Generic, Show)

instance Exception TableJSONError
  


input2 :: LByteString
input2 = [q|
[
   {
      "Name" : "Xytrex Co.",
      "Description" : "Industrial Cleaning Supply Company",
      "Account Number" : "ABC15797531",
      "b" :null
   },
   {
      "Name" : "Watson and Powell, Inc.",
      "Account Number" : null,
      "b" : 2.3,
      "Description" : "Industrial Cleaning Supply Company"
   }
]
|]

input1 :: LByteString
input1 = [q|
[
   {
      "Account Number" : "ABC15797531",
      "b" :null
   },
   {
      "Account Number" : null,
      "b" : 2.2
   }
]
|]

input3 :: LByteString
input3 = [q|
{
   "k1" : {
      "c1" : "",
      "c2" :null
   },
   "k2" : {
      "c2" : null,
      "c1" : 2.2
   }
,
   "iun" : {
      "c1" : null,
      "c2" : ""
   }
}
|]


 
parseInput1 :: IO ()
parseInput1 = either (\x -> print True >> putStrLn x)
  (maybe (print ("empty table"::Text)) (\x -> print (meta x) >> print x) . fromVectorOfRecords) $ eitherDecode input1 
 
parseKeyToRecordMap1 :: IO ()
parseKeyToRecordMap1 = either (\x -> print True >> putStrLn x)
  (maybe (print ("empty table"::Text)) (\x -> print (meta $ value x) >> print x) . fromKeyToRecordMap) $ eitherDecode input3

nulls1 :: IO ()
nulls1 = either (\x -> print True >> print x)
  (maybe (print ("empty table"::Text)) (\x -> print (meta $ value x) >> print x)) $
  (eitherDecodeWith json' $ iparse $ parseKeyToRecordMap [""]) input3

decodeVectorOfRecordsWith :: [Text] -> LByteString -> Either String (Maybe Table)
decodeVectorOfRecordsWith nulls = (_Left %~ snd) . (eitherDecodeWith json' $ iparse $ parseVectorOfRecords nulls)

decodeKeyToRecordMapWith :: [Text] -> LByteString -> Either String (Maybe (SingleKeyTable Text))
decodeKeyToRecordMapWith nulls = (_Left %~ snd) . (eitherDecodeWith json' $ iparse $ parseKeyToRecordMap nulls)

-- main = parseKeyToRecordMap1
-- main = nulls1
-- main = parseInput1

-- | JSON type of a value, name of the head constructor.
aesonTypeOf :: Value -> String
aesonTypeOf v = case v of
    Object _ -> "Object"
    Array _  -> "Array"
    String _ -> "String"
    Number _ -> "Number"
    Bool _   -> "Boolean"
    Null     -> "Null"
