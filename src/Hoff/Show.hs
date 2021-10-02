{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
module Hoff.Show where 

import           Data.SOP
import qualified Data.Text as TS
import           Data.Vector (Vector)
import qualified Data.Vector as VB
import qualified Data.Vector.Generic as V
import qualified TextShow
import           TextShow hiding (fromString)
import           Yahp as Y

-- * explanations
-- buildAtomRaw:
--
-- buildAtomInColumn:
-- used for 
  
instance Show a => AtomShow a where
  buildAtomRaw x = TextShow.fromString $ show x -- <> "_default"
  
-- | minimal complete definition: buildAtomRaw
-- typical for scalar types to also define buildAtomInColumn
class AtomShow v where

  -- | used for elements of a vector when the vector is shown on a single line delimited by brackets
  buildAtomRaw   :: v -> Builder
  default buildAtomRaw :: TextShow v => v -> Builder
  buildAtomRaw = showb

  -- | used for elements of a vector when shown in multiline mode (e.g. in a table column or dict)
  buildAtomInColumn :: v -> Builder
  buildAtomInColumn = buildAtomRaw

  showNothing :: Proxy v -> Text
  showNothing _ = "Nothing"

  -- showJustAtom :: v -> Text
  -- showJustAtom v = "Just " <> showAtomInGeneralList v

  buildSingleton  :: Proxy v -> Builder -> Builder
  buildSingleton _ = id

  buildSingleLineVector  :: Proxy v -> Vector Builder -> Builder
  buildSingleLineVector _ = buildBracketList

  defaultIsSingleLine           :: Proxy v -> Bool
  defaultIsSingleLine _ = True

-- | a class defining the same core operations as AtomShow
-- where the implementatino can leverage the AtomShow instance of the wrapped type
class AtomShow1 g where
  buildAtomInColumn1    :: AtomShow a => g a -> Builder
  buildAtomRaw1         :: AtomShow a => g a -> Builder

instance AtomShow1 Maybe where
  buildAtomInColumn1    = maybe mempty buildAtomInColumn
  buildAtomRaw1         :: forall a . AtomShow a => Maybe a -> Builder
  buildAtomRaw1       x = maybe (fromText $ showNothing $ Proxy @a) buildAtomRaw x

instance AtomShow1 I where
  buildAtomInColumn1    = buildAtomInColumn . unI
  buildAtomRaw1         = buildAtomRaw . unI

instance AtomShow l => AtomShow1 (Either l) where
  buildAtomInColumn1    = eitherBuild buildAtomInColumn
  buildAtomRaw1         = eitherBuild buildAtomRaw

eitherBuild :: (AtomShow a, AtomShow b) => (forall a . AtomShow a => a -> Builder) -> Either a b -> Builder
eitherBuild f = either (mappend (fromText "L/") . f) (mappend (fromText "R/") . f)


class ValueShow v where
  buildSingleLine :: v -> Builder

  -- | actual (top-level) showing of the value
  showv :: HasCallStack => v -> Text
  
class ValueShow v => DictComponentShow v where
  getDictBuilder :: (HasCallStack) => v -> InDictBuilder

class ValueShow v => SingleColumnShow v where
  buildSingleColumn :: HasCallStack => v -> Vector Builder


data InDictBuilder = InDictBuilder { hWidth                 :: Int
                                   , hHeader                :: Builder
                                   , hNonEmtpyHeader        :: Bool
                                   , hBody                  :: Vector Builder }

singleColumnInDict :: (HasCallStack, SingleColumnShow v) => Maybe Text -> v -> InDictBuilder
singleColumnInDict header = fitToWidth header . buildSingleColumn


unlinesV :: (ConvertText LText c, V.Vector v Builder) => v Builder -> c
unlinesV = builderToText . unlinesB . V.toList

combine :: InDictBuilder -> VB.Vector Builder
combine (InDictBuilder w h ne b) = bool b (V.fromList [h, fromText $ TS.replicate w "-"] <> b) ne

horizontalConcat :: Text -> NonEmpty InDictBuilder -> InDictBuilder
horizontalConcat sep = foldl1 comb
  where comb (InDictBuilder w1 h1 ne1 b1) (InDictBuilder w2 h2 ne2 b2) =
          InDictBuilder (w1 + w2 + TS.length sep) (comb2 h1 h2) (ne1 || ne2) $ V.zipWith comb2 b1 b2
        comb2 a b = a <> fromText sep <> b


fitToWidth :: HasCallStack => Maybe Text -> VB.Vector Builder -> InDictBuilder
fitToWidth header texts = InDictBuilder width headerT (isJust header) $ V.zipWith pad lengths texts
  where lengths = fromIntegral . lengthB <$> texts
        width = maybe id (max . TS.length) header $ if V.null lengths then 0 else V.maximum lengths
        pad l t = t <> fromText (TS.replicate (width - l) " ")
        headerT = let h = fromMaybe "" header in pad (TS.length h) $ fromText h

singleLineWithSuffix :: V.Vector v a => (a -> Builder) -> Char -> v a -> Builder
singleLineWithSuffix f s l = V.foldMap f l <> singleton s


buildBracketList :: Foldable t => t Builder -> Builder
buildBracketList bs = singleton '[' <> (mconcat $ Y.intersperse (fromText ", ") $ toList bs ) <> singleton ']' 

showSingleLine :: (ConvertText LText c, ValueShow a) => a -> c
showSingleLine = builderToText . buildSingleLine

builderToText = toS . toLazyText
