
import pandas as pd
import numpy as np
from pathlib import Path
import cbor2

def default_encoder(e,v):
    if isinstance(v, pd._libs.missing.NAType): return e.encode(None)
    if isinstance(v, np.int64): return e.encode(v.item())
    assert False, str(type(v)) + ": " + str(v) 

def myCborDumps(obj):
    return cbor2.dumps(obj) #, default=default_encoder)

def myCborDump(obj, f):
    return cbor2.dump(obj, f) #, default=default_encoder)


def toHoffCbor(dfs, fileLike = None):
    obj = [[name, [list(df), [seriesToHoffCbor(df, df[s]) for s in df]]] for name,df in dfs]
    # print(obj)
    if fileLike is None:
        return myCborDumps(obj)
    return myCborDump(obj, fileLike)

def seriesToHoffCbor(df, s):
    (colType, func, nullable) = fromDtype[s.dtype]
    if callable(colType): colType = colType(df, s)

    # NO! the following is needed, or otherwise s2.loc[na]=None leads to the warning '
    # 'a value is trying to be set on a copy of a slice from a dataframe'0
    # if len(s) == 0: return [colType,[]]
    # YES! 
    # instead: s2=s.copy(). the problem was that some column in a df returned from read_csv had ._is_view = True
    # s2.loc[na] would modify the original df, which is never a good idea anyway

    # NO! this is not enough, because an object column can always hide nan's that are not exactly 'None'
    # this is needed to handle this:
    # toHoffCbor([('a',pd.DataFrame({'a':[np.NaN,None]}, dtype='str'))]).hex()
    # if colType == 'I None':
        # return [colType,[None]*len(s)]
    # YES! a real solution was s2.loc[na]=None

    na = s.isna()

    if nullable and na.any():
        # turn anything nullable into an object and set proper None's
        # and apply the function to non-na only (e.g. to get proper
        # ints, supported by cbor)
        if s.dtype != object:
            s2 = s.astype(object)
        else:
            s2 = s.copy()
        s2.loc[na]=None
        if not func is None:
            s2.loc[~na]=func(df,s[~na])
    else:
        s2 = s if func is None else func(df, s)

    # print(s2.dtype, s2.isna().any())
    return [colType,list(s2)]

def decideObject(df, x):
    notna = x[x.notna()]
    if notna.empty:
        return 'I None'

    colType = type(notna.iloc[0])

    wrongType = notna.map(lambda y: type(y)!=colType)

    if wrongType.any():
        y = df[x.notna()][wrongType].copy()
        y['typeError'] = y[x.name].map(lambda v: str(type(v)) + " " + str(v))
        assert not wrongType.any(), f"column {x.name} contains non-'{colType}' objects :\n{y}"
    
    return recognizedObjectTypes[colType]

def decideDay(df, col):
    try:
        if (col.isna() | (col.dt.normalize() == col)).all():
            return 'Maybe Day'
        return 'Maybe Time'
    except Exception as e:
        raise Exception(f"when handling column {col.name}") from e


    # pandas' disgusting auto casting, means that anything can be
    # anything, especially anything can be Na, even if something that
    # is non-nullable int64 NOW will simply not be that if a Na shows
    # up
fromDtype = {np.dtype('int64')          : ('Maybe Int64'        ,None                           , False)
             ,pd.Int64Dtype()           : ('Maybe Int64'        ,lambda _,x: x.astype(int)      , True)
             ,np.dtype('int32')         : ('Maybe Int64'        ,None                           , False)
             ,pd.Int32Dtype()           : ('Maybe Int64'        ,lambda _,x: x.astype(int)      , True)
             ,np.dtype('bool')          : ('Maybe Bool'         ,None                           , False)
             ,np.dtype('O')             : (decideObject         ,None                           , True)
             ,np.dtype('float64')       : ('Maybe Double'       ,None                           , True)
             ,np.dtype('<M8[ns]')       : (decideDay            ,lambda _, x: x.astype(int)     , True)
             }

recognizedObjectTypes = {bool: 'Maybe Bool'
                         ,str: 'Maybe Text'
                         }

fromHaskellType = {'I Int64'     : (pd.Int64Dtype()              , None)
                  ,'I Int'       : (pd.Int64Dtype()              , None)
                  ,'I Text'      : (np.dtype('O')                , None)
                  ,'I [Char]'    : (np.dtype('O')                , None)
                  ,'I Char'      : (np.dtype('O')                , None)
                  ,'I Double'    : (np.dtype('float64')          , None)
                  ,'I Bool'      : (np.dtype('bool')             , None)
                  ,'I Time'      : (np.dtype('<M8[ns]')          , pd.to_datetime)
                  ,'I Day'       : (np.dtype('<M8[ns]')          , pd.to_datetime)
                  ,'I None'      : (np.dtype('O')                , None)
                  ,'Maybe Int64' : (pd.Int64Dtype()              , None)
                  ,'Maybe Int'   : (pd.Int64Dtype()              , None)
                  ,'Maybe Text'  : (np.dtype('O')                , None)
                  ,'Maybe [Char]': (np.dtype('O')                , None)
                  ,'Maybe Char'  : (np.dtype('O')                , None)
                  # ,'Maybe ByteString'  : (np.dtype('O')          , tobase64?)
                  ,'Maybe Double': (np.dtype('float64')          , None)
                  ,'Maybe Bool'  : (np.dtype('O')                , None) # only difference
                  ,'Maybe Time'  : (np.dtype('<M8[ns]')          , pd.to_datetime)
                  ,'Maybe Day'   : (np.dtype('<M8[ns]')          , pd.to_datetime)
                   }


def example():
    a = pd.DataFrame({'IntCol': [1,10], 'BoolCol':[True,False], 'TextCol':['sd','yi'], 'DoubleCol':[1.3, None],
                      # 'd': pd.Series([1,None], dtype='Int64')
                      })
    a['TimestampCol'] = pd.to_datetime(a.IntCol)
    print(a.dtypes)
    print({a[c].dtype:c for c in a})
    print(toHoffCbor(a).hex())
    return a

def toHoffFile(df, path):
    Path(path).write_bytes(toHoffCbor(df))


# arg should be bytes or fileLike
def fromHoffCbor(arg):
    if type(arg) == bytes:
        obj = cbor2.loads(arg)
    else:
        obj = cbor2.load(arg)

    cols={}
    for (k,v) in zip(obj[0], obj[1]):
        # print(k)
        # print(v[0])
        # print(v[1])
        (dtype, func) = fromHaskellType[v[0]]
        s = pd.Series(data = v[1], dtype=dtype)
        cols[k] = s if func is None else func(s)

    return pd.DataFrame(cols)
