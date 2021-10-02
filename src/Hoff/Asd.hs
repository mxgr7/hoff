{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE PatternSynonyms    #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
module Hoff.Asd  where


import qualified Chronos as C
import           Control.Exception
import           Control.Lens
import           Data.Coerce
import           Data.Functor.Classes as Reexport
import qualified Data.HashMap.Strict as HM
import qualified Data.HashTable.ST.Cuckoo as HT
import           Data.Hashable
import           Data.Hashable.Lifted (Hashable1, hashWithSalt1)
import qualified Data.List
import qualified Data.Maybe
import           Data.Record.Anon as Reexport (KnownFields, pattern (:=), K(..), AllFields, RowHasField)
import           Data.Record.Anon.Advanced (Record)
import qualified Data.Record.Anon.Advanced as R
import           Data.SOP
import           Data.String
import qualified Data.Text as T
import           Data.Type.Equality
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Generic as G
import           Hoff.Dict as H
import           Hoff.HSql
import           Hoff.Show
import           Hoff.Table
import           Hoff.Utils
import           Hoff.Vector
import qualified Prelude
import           TextShow as B
import           Type.Reflection as R
import           Unsafe.Coerce ( unsafeCoerce )
import           Yahp as P hiding (get, group, groupBy, (&), TypeRep, typeRep, Hashable, hashWithSalt, (:.:), null)


a :: (:.:) Vector Vector Int
a = Comp $ fmap toVector $ toVector [[1,2],[3::Int]]

b :: WrappedDyn (Vector :.: Vector)
b = toWrappedDynI a

c = UnsafeTableWithColumns $ dict #a $ toVector [b]

group2 :: TableCol -> WrappedDyn (Vector :.: Vector)
group2 (WrappedDyn _ v) = WrappedDyn typeRep $ Comp $ fmap pure v

ungroup :: WrappedDyn (Vector :.: Vector) -> TableCol
ungroup (WrappedDyn _ v) = WrappedDyn typeRep $ V.concat $ toList $ unComp v

d :: WrappedDyn (Vector :.: Vector)
d = group2 $ toWrappedDynI $ toVector [1::Int,2,3]

instance Show (WrappedDyn (Vector :.: Vector)) where
  show = withWrapped $ Prelude.unlines . toList . fmap g . unComp
    where g v = toS . toLazyText . buildSingleLineVector (vectorProxy v) $ buildAtomRaw1 <$> v
