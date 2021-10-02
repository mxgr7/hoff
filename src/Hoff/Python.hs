{-# LANGUAGE QuasiQuotes #-}
module Hoff.Python where

import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as BU
import           Foreign ( castPtr )
import           Hoff.Serialise
import           Hoff.Table
import qualified Prelude
import           System.Exit
import           System.FilePath
import qualified System.IO as S
import           System.IO.Error ( userError )
import           System.Posix.IO.ByteString hiding (fdWrite)
import           System.Posix.Types
import           System.Process hiding (createPipe)
import           Text.InterpolatedString.Perl6
import           Yahp

cborToTables :: ByteString -> Either Text [(Text, Table)]
cborToTables b = do
  x <- tableDeserialise $ toS b
  seq x $ pure x

cborToTable :: ByteString -> Either Text Table
cborToTable  b = do
  x <- tableDeserialise $ toS b
  case x of
    [(k, y)] | k == singleTableKey      -> seq y $ pure y
    y                                   -> Left $ "Impossible python results: " <> toS (show $ fmap (\(n,_) -> n) y)

singleTableKey :: Text
singleTableKey = "single table output" 

singleTableSourceMod :: ConvertText a Text => a -> Text
singleTableSourceMod x = toS x <> "\noutputDfs = [['" <> singleTableKey <> "', outputDf]]\n"

-- | this uses the given Python binary to execute a script which can expect the inputTable converted
-- to a dataframe stored in python variable 'inputDf' and is expected to store its result dataframes
-- in the python variable 'outputDfs` as a list of pairs `(name, dataframe)`.
pythonEvalHoffs :: (HasCallStack, ConvertText t Text)
  => FilePath -> t -> [String] -> Maybe [(String, String)] -> Maybe Table -> IO (Either Text [(Text, Table)])
pythonEvalHoffs x y z w v = cborToTables <$> pythonEvalHoffCbor x y z w v

-- | same as `pythonEvalHoffs` for a single result table stored in `outputDf`
pythonEvalHoff :: (HasCallStack, ConvertText t Text)
  => FilePath -> t -> [String] -> Maybe [(String, String)] -> Maybe Table -> IO (Either Text Table)
pythonEvalHoff x source z w v = cborToTable <$> pythonEvalHoffCbor x (singleTableSourceMod source) z w v

pythonEvalHoffCbor :: (HasCallStack, ConvertText t Text)
  => FilePath -> t -> [String] -> Maybe [(String, String)] -> Maybe Table -> IO ByteString
pythonEvalHoffCbor pythonBin source args env inputTableM = do
  modulePath <- Hoff.Serialise.pythonModule
  pythonEvalByteString pythonBin (importHoff modulePath <> inputDf <> (toS source :: Text) <> outputDfs) args env $ toS . serialise <$> inputTableM
  where importHoff modulePath = [qc|
import sys
import pandas as pd
sys.path.append('{takeDirectory modulePath}')
# print(sys.path)
import {takeBaseName modulePath} as HoffSerialise
outputDfFile = sys.argv[1]
del sys.argv[1]
# print(HoffSerialise)

|]
        inputDf = bool "" "\ninputDf = HoffSerialise.fromHoffCbor(sys.stdin.buffer)\n" $ isJust inputTableM
        outputDfs = [qc|
with open(outputDfFile, 'wb') as f:
  HoffSerialise.toHoffCbor(outputDfs, f)

|]

-- | this uses the given Python binary to execute a script which is expected to write its result to
-- a file descriptor, whose path (i.e. "/dev/fd/X") is passed as the first arg to python
pythonEvalByteString :: (HasCallStack, ConvertText t Text)
  => FilePath -> t -> [String] -> Maybe [(String, String)] -> Maybe ByteString -> IO ByteString
pythonEvalByteString pythonBin source args env inputM = do
  (outputRead, outputWrite) <- createPipe
  (scriptRead, scriptWrite) <- createPipe

  forkIO $ do fdWrite scriptWrite $ "import sys\n" <> toUtf8 source 
              closeFd scriptWrite

  threadWaitRead scriptRead

  let procDescr = (proc pythonBin $ [fdPath scriptRead, fdPath outputWrite] <> args)
        { std_in = bool NoStream CreatePipe $ isJust inputM, env = env }
  withCreateProcess procDescr $ \stdinH _ _ procH -> do
    forkIO $ waitForProcess procH >> mapM_ closeFd [outputWrite, scriptRead]

    forM inputM $ \input -> seq input $ forMaybe (Prelude.error "did you set std_in = CreatePipe?") stdinH $
      \h -> forkIO $ do S.hSetBinaryMode h True -- not sure if this is needed
                        B.hPut h input
                        S.hClose h

    -- print "waiting for python output"
    output <- B.hGetContents =<< fdToHandle outputRead
    waitForProcess procH >>= \case
      ExitFailure code -> throwIO $ userError $ "python Exit code: " <> show code
      ExitSuccess -> pure output

fdPath :: Show a => a -> String
fdPath fd = "/dev/fd/" <> show fd
                                   
  
-- | Write a 'ByteString' to an 'Fd'.
fdWrite :: Fd -> ByteString -> IO ByteCount
fdWrite fd bs = BU.unsafeUseAsCStringLen bs
  $ \(buf,len) -> fdWriteBuf fd (castPtr buf) (fromIntegral len)

        
