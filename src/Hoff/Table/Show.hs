{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
module Hoff.Table.Show where

import           Data.Record.Anon.Advanced (Record)
import           Data.SOP
import qualified Data.Vector as V
import           Hoff.Dict as H
import           Hoff.Table.Types
import           Hoff.Show
import qualified Prelude
import           TextShow as B hiding (fromString)
import           Yahp as P

instance (KnownFields r, AllFields r (Compose Show I)) => TextShow (Record I r) where
  showbPrec _ = fromText . toS . show
  {-# INLINE showbPrec #-}
  
instance {-# OVERLAPPING #-} (KnownFields r, AllFields r (Compose Show I)) => AtomShow (Record I r) where
  buildSingleLineVector _ = unwordsB . toList
  {-# INLINE buildSingleLineVector #-}

instance {-# OVERLAPPING #-} AtomShow TableCell where
  buildAtomRaw = withWrapped (\(I (x :: g a)) -> buildSingleton (Proxy @a) $ buildAtomRaw1 x)
  defaultIsSingleLine _ = False

-- instance {-# OVERLAPPING #-} AtomShow GroupedCol where
  -- buildAtomRaw = buildSingleLine
  -- defaultIsSingleLine _ = False

instance {-# OVERLAPPING #-} AtomShow TableCol where
  buildAtomRaw = buildSingleLine
  defaultIsSingleLine _ = False

-- instance SingleColumnShow GroupedCol where
  -- buildSingleColumn = undefined -- withWrapped $ fmap buildSingleLine . unComp

instance SingleColumnShow TableCol where
  buildSingleColumn = withWrapped $ fmap buildAtomInColumn1

-- | TODO: in q a column (or more precisely a vector) whose elements are all vectors of the same length is shown in a grid (like a table) 
instance ValueShow TableCol where
  buildSingleLine = withWrapped $ \v ->
    buildSingleLineVector (vectorProxy v) $ buildAtomRaw1 <$> v
  showv col = if withWrapped (defaultIsSingleLine . vectorProxy) col then toS . toLazyText $ buildSingleLine col
              else unlinesV $ buildSingleColumn col

-- instance ValueShow GroupedCol where
  -- buildSingleLine = undefined -- withWrapped $ \(Comp v) ->
    -- buildSingleLineVector (vectorProxy v) $ buildAtomRaw1 <$> v
  -- showv col = if withWrapped (defaultIsSingleLine . vectorProxy) col then toS . toLazyText $ buildSingleLine col
              -- else unlinesV $ buildSingleColumn col

-- instance DictComponentShow GroupedTableRaw where
  -- getDictBuilder t      = maybe (throwH $ TableWithNoColumn "cannot show") (horizontalConcat " ")
    -- $ nonEmpty $ V.toList $ zipTable (singleColumnInDict . Just) t
  
instance DictComponentShow Table where
  getDictBuilder t      = maybe (Prelude.error "impossible: someone constructed a table with no columns") (horizontalConcat " ")
    $ nonEmpty $ V.toList $ zipTable (singleColumnInDict . Just) t
  
-- | I do not think this is needed or used in Q
-- instance SingleColumnShow Table where
--   buildSingleColumn = fmap buildSingleLine . rows

-- instance ValueShow GroupedTableRaw where
  -- buildSingleLine t = "+" <> buildSingleLine (flipTable t) 
  -- showv = unlinesV . combine . getDictBuilder

instance ValueShow Table where
  buildSingleLine t = "+" <> buildSingleLine (flipTable t) 
  showv = unlinesV . combine . getDictBuilder


instance Show Table where show = toS . showv
instance Show TableCol where show = toS . showv
instance Show TableCell where show = toS . showSingleton

showSingleton :: forall a . AtomShow a => a -> Text
showSingleton = toS . toLazyText . buildSingleton (Proxy @a) . buildAtomRaw
{-# INLINE showSingleton #-}

deriving via Table instance Show (TypedTable r)
deriving via KeyedTable instance Show (KeyedTypedTable k v)
