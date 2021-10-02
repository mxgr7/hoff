
module Hoff.Serialise
  (module Hoff.Serialise
  ,module Codec.Serialise
  )
where

import qualified Chronos as C
import           Codec.CBOR.Decoding
import           Codec.CBOR.FlatTerm as C
import           Codec.CBOR.Pretty as C
import           Codec.CBOR.Read as C
import           Codec.CBOR.Term as C
import           Codec.Serialise
import           Codec.Serialise.Class
import           Codec.Serialise.Encoding
import           Control.Lens
import           Control.Monad.Writer
import           Data.Coerce
import           Data.Maybe
import           Data.SOP
import qualified Data.Text as T
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import           Hoff.Dict
import           Hoff.H
import           Hoff.Table
import           Hoff.Utils
import           Paths_hoff (getDataFileName)
import qualified Prelude as Unsafe
import qualified Text.Hex as H
import           Text.Pretty.Simple
import           Type.Reflection
import           Yahp as Y hiding (TypeRep, typeRep)

-- tablesFromHex :: Text -> [(Text, Table)]
-- tablesFromHex = deserialise . fromHex

toDiagnostic :: Text -> String
toDiagnostic = toDiagnostic' . deserialise @C.Term . fromHex

printDiagnostic :: Text -> IO ()
printDiagnostic = putStrLn . toDiagnostic


toDiagnostic' :: Serialise a => a -> String
toDiagnostic' = C.prettyHexEnc . encode 

printDiagnostic' :: Serialise a => a -> IO ()
printDiagnostic' = putStrLn . toDiagnostic'

colTypeSelect :: (forall a . (Wrappable a, Serialise a) => TypeRep a -> Decoder s x) -> Text -> Decoder s x
colTypeSelect f = \case
              "Int64"   -> f $ typeRep @Int64
              "Text"    -> f $ typeRep @Text
              "Double"  -> f $ typeRep @Double
              "Bool"    -> f $ typeRep @Bool
              "Time"    -> f $ typeRep @C.Time
              -- "Time"    -> f $ typeRep @Chronos.Day
              x         -> fail $ "column type '" <> toS x <> "' not implenmented"
              

deriving instance Serialise C.Time
instance Serialise None where
  encode _ = encodeNull
  decode = coerce decodeNull

instance Hashable C.Datetime where
  hashWithSalt s = hashWithSalt s . C.datetimeToTime


encodeVector' :: (VG.Vector v a) => (a -> Encoding) -> v a -> Encoding
encodeVector' encode = encodeContainerSkel encodeListLen VG.length VG.foldr (\a b -> encode a <> b)
{-# INLINE encodeVector' #-}

decodeVector' :: (VG.Vector v a) => Decoder s a -> Decoder s (v a)
decodeVector' decode = bind decodeListLen $ \size -> VG.replicateM size decode
{-# INLINE decodeVector' #-}

data DiagTable = DiagTable (Vector Symbol) (Vector Term)
-- data D = D (Vector Symbol) (Vector TableCol)
  deriving Show

diagDeserialiseFromHex :: Text -> IO ()
diagDeserialiseFromHex = putStrLn . unlines . tableDeserialiseErrors . fromHex

-- λ> diagDeserialiseFromHex "828261618282616164636f6c3282826a4d61796265205465787482f97e0063617364826a4d61796265205465787482f97e006361736482667461626c65328282616164636f6c3282826a4d61796265205465787482f97e0063617364826a4d61796265205465787482f97e0063617364"
-- In table 'a': Column 'a' of type 'Maybe Text': decodeString: unexpected token TkFloat16 NaN
-- In table 'a': Column 'col2' of type 'Maybe Text': decodeString: unexpected token TkFloat16 NaN
-- In table 'table2': Column 'a' of type 'Maybe Text': decodeString: unexpected token TkFloat16 NaN
-- In table 'table2': Column 'col2' of type 'Maybe Text': decodeString: unexpected token TkFloat16 NaN

tableDeserialise :: LByteString -> Either Text [(Text, Table)]
tableDeserialise bs = either (\e -> Left $ unlines $ [toS $ show e,"Diagnostics:"] <>  tableDeserialiseErrors bs)
  pure $ deserialiseOrFail bs

tableDeserialiseErrors :: LByteString -> [Text]
tableDeserialiseErrors = either (\e -> ["Error parsing CBOR in tableDeserialiseErrors: " <> toS (show e)])
  (execWriter . mapM_ @[] g) . deserialiseOrFail
  where g (tableName, DiagTable colNames cols) = void $
          mapWriter (second $ fmap (("In table '" <> tableName <> "': " :: Text ) <>)) $
          tellErrors $ V.zipWith f colNames cols
        f colName c@(TList [TString colType,_]) =
          (_Left %~ (\x -> "Column '" <> colName <> "' of type '" <> colType <> "': " <> toS x))
          $ fromFlatTerm decodeColPython $ toFlatTerm $ encode c
        f colName x = Left $ "Column '" <> colName <> "' could not be parsed. Data[:1000]:\n" <> (toS $ pShow x)

deriving instance Serialise a => Serialise (I a)

instance Serialise DiagTable where 
  encode = Unsafe.error "Serialise DiagTable not implemented"
  decode = decodeListLenOf 2 >> DiagTable <$> decode <*> decode -- decodeVector' dec
    -- where dec = decodeListLenOf 2 >> (,) <$> decode @Text <*> decodeVector' decodeTermToken

instance Serialise Table where 
  encode = (\t -> encodeListLen 2 <> encode (key t) <> encodeVector' encodeColPython (value t)) . flipTable
  decode = decodeListLenOf 2 >> do
    k <- decode
    v <- decodeVector' decodeColPython
    runHWith (fail . show) pure $ table2 =<< dict k v

encodeColPython :: (HasCallStack) => TableCol -> Encoding
encodeColPython (WrappedDyn tr wd) = encodeListLen 2 <> encode (show tr) <> dat
  where dat
          | Just HRefl <- tr `eqTypeRep` typeRep @(I Int64)     = encode wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(I Int)       = encode wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(I Text)      = encode wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(I String)    = encode $ T.pack <$> coerceUI wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(I Char)      = encode $ T.singleton <$> coerceUI wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(I Double)    = encode wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(I Bool)      = encode wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(I C.Time)    = encode wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(I C.Day)     = encode $ C.dayToTimeMidnight <$> coerceUI wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(I None)      = encode wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe C.Day) = encodeVectorPythonMaybe $ fmap2 C.dayToTimeMidnight wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Int64) = encodeVectorPythonMaybe wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Int)   = encodeVectorPythonMaybe wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Text)  = encodeVectorPythonMaybe wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe String)= encodeVectorPythonMaybe $ fmap2 T.pack wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Char)  = encodeVectorPythonMaybe $ fmap2 T.singleton wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Double)= encodeVectorPythonMaybe wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Bool)  = encodeVectorPythonMaybe wd
          | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe C.Time)= encodeVectorPythonMaybe wd
          | True                                                = Unsafe.error
            $ "encodeColPython not implemented for columns of type " <> show tr

encodeVectorPythonMaybe :: (Serialise a, VG.Vector v (Maybe a)) => v (Maybe a) -> Encoding
encodeVectorPythonMaybe = encodeVector' $ maybe encodeNull encode

decodeVectorPythonMaybe :: (Serialise a, VG.Vector v (Maybe a)) => Decoder s (v (Maybe a))
decodeVectorPythonMaybe = decodeVector' $ do tkty <- peekTokenType
                                             case tkty of TypeNull -> Nothing <$ decodeNull
                                                          _        -> Just <$> decode

decodeColPython :: HasCallStack => Decoder s TableCol
decodeColPython = decodeListLenOf 2 >> decode @Text >>= \case
  "I Int64"      -> fI @Int64    decode
  "I Int"        -> fI @Int      decode
  "I Bool"       -> fI @Bool     decode
  "I Text"       -> fI @Text     decode
  "I [Char]"     -> fI $ fmap2 T.unpack decode
  "I Double"     -> fI @Double   decode
  "I Time"       -> fI @C.Time   decode
  "I Day"        -> fI $ fmap2 C.timeToDayTruncate decode
  "I None"       -> fI @None     decode
  "Maybe Int64"  -> fM $ decodeVectorPythonMaybe @Int64
  "Maybe Int"    -> fM $ decodeVectorPythonMaybe @Int
  "Maybe Bool"   -> fM $ decodeVectorPythonMaybe @Bool
  "Maybe Text"   -> fM $ decodeVectorPythonMaybe @Text
  "Maybe [Char]" -> fM $ fmap3 T.unpack $ decodeVectorPythonMaybe
  "Maybe Double" -> fM $ decodeVectorPythonMaybe @Double
  "Maybe Time"   -> fM $ decodeVectorPythonMaybe @C.Time
  "Maybe Day"    -> fM $ fmap3 C.timeToDayTruncate $ decodeVectorPythonMaybe
  colType        -> Unsafe.error
            $ "decodeColPython not implemented for columns of type " <> toS colType
  where fI :: (Wrappable a) => Decoder s (V.Vector a) -> Decoder s TableCol
        fI = fmap toWrappedDynI
        fM :: (Wrappable a) => Decoder s (f (Maybe a)) -> Decoder s (WrappedDyn f)
        fM = fmap toWrappedDyn

-- | generated with:
-- 
-- y=cbor2.dumps([["c1", ["IntCol",[1,2]]], ["c2", ["DoubleCol", [1.2, 10.29387429837239487]]], ["d", ["TextCol",["asd",""]]]])
-- print(y.hex())
--
-- result: 
-- 
-- Columns: ["c1","c2","d"]
-- Values: [[1,2],[1.2,10.293874298372394],["asd",""]]
example = do
  -- let hex = "828566496e74436f6c67426f6f6c436f6c6754657874436f6c69446f75626c65436f6c6c54696d657374616d70436f6c858266496e74436f6c8201028267426f6f6c436f6c82f5f4826754657874436f6c826273646279698269446f75626c65436f6c82fb3ff4cccccccccccdfb400199999999999a826c54696d657374616d70436f6c820102" 
  let hex = "828566496e74436f6c67426f6f6c436f6c6754657874436f6c69446f75626c65436f6c6c54696d657374616d70436f6c858266496e74436f6c82010a8267426f6f6c436f6c82f5f4826754657874436f6c826273646279698269446f75626c65436f6c82fb3ff4cccccccccccdf97e00826c54696d657374616d70436f6c82010a" 
  -- let hex = "828466496e74436f6c67426f6f6c436f6c6754657874436f6c69446f75626c65436f6c848266496e74436f6c8201028267426f6f6c436f6c82f5f4826754657874436f6c826273646279698269446f75626c65436f6c82fb3ff4cccccccccccdfb400199999999999a" 
  printDiagnostic hex
  -- let t = deserialise $ fromHex hex :: Table
  -- print t
  -- print $ select [ #k <. fmap (Chronos.encodeIso8601 . Chronos.timeToDatetime) "TimestampCol"] t
  -- pure t


fromHex :: Text -> LByteString
fromHex = toS . fromJust . H.decodeHex


pythonModule :: IO FilePath
pythonModule = getDataFileName "python/HoffSerialise.py"
