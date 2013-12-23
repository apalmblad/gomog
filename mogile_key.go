package gomog

import( 
  "strconv"
  "net/url"
  "net/http"
  "fmt"
  "io"
  "math/rand"
  )

type MogileKey struct {
  Key string
  Domain *MogileDomain
}

type MogileKeyList struct {
  Keys []*MogileKey
  LastKey string
}

// KEYS
func ( k *MogileKey ) values() url.Values {
  v := k.Domain.values()
  v.Add( "key", k.Key )
  return v
}
func ( k *MogileKey ) FileInfo() ( url.Values, error ) {
  return k.Domain.client.doRequest( "file_info", k.values(), true )
}

func ( k *MogileKey ) Paths() ( []string, error ) {
  data, err := k.Domain.client.doRequest( "get_paths", k.values(), true )
  numPaths, _ := strconv.Atoi( data.Get( "paths" ) )
  rVal := make( []string, numPaths )
  for i := 0; i < numPaths; i++ {
    rVal[i] = data.Get( "path" + strconv.Itoa(i+1) )
  }
  return rVal, err
}

func ( k *MogileKey ) Path() ( string, error ) {
  paths, err := k.Paths()
  if( err != nil ) {
    return "", err
  }
  return paths[ rand.Int31n( int32( len( paths ) )  ) ], nil
}

func ( d *MogileDomain ) Key( key string ) *MogileKey {
  var k MogileKey
  k.Domain = d
  k.Key = key
  return &k
}

func ( k *MogileKey ) ListFids( ) ( []*MogileFid, error ) {
  v := k.values()
  data, err := k.Domain.client.doRequest( "list_fids", v, true )
  numFids, _ := strconv.Atoi( data.Get( "fid_count" ) )
  rVal := make( [](*MogileFid), numFids );
  for i := 0; i < numFids; i++ {
    var f MogileFid 
    base := "fid_" + strconv.Itoa( i+1 )
    f.Domain = data.Get( base + "_domain" )
    f.DevCount, _ = strconv.Atoi( data.Get( base + "_devcount" ) )
    f.Size, _ = strconv.ParseInt( data.Get( base + "_length" ), 10, 64 )
    f.Class = data.Get( base + "_class" )
    f.Key = data.Get( base + "_key" )
    f.FileId, _ = strconv.Atoi( data.Get( base + "_fid" ) )
    rVal[i] = &f
  }
  return rVal, err
}
func ( k *MogileKey ) Delete() ( error ) {
  _, err := k.Domain.client.doRequest( "delete", k.values(), false )
  return err
}

func ( k *MogileKey ) Rename( newName string ) ( error ) {
  v := k.Domain.values()
  v.Add( "from_key", k.Key )
  v.Add( "to_key", newName )
  _, err := k.Domain.client.doRequest( "rename", v, false )
  if( err != nil ) {
    k.Key = newName
  }
  return err
}
func ( k *MogileKey ) CreateClose( fid *MogileFid, path string, size int64 ) error {
  v := k.values()
  v.Add( "fid", strconv.Itoa( fid.FileId ) )
  v.Add( "devid", strconv.Itoa( fid.DeviceId ) )
  v.Add( "path", path )
  if( size > 0  ) {
    v.Add( "size", strconv.FormatInt( size, 10 ) )
  }
  _, err := k.Domain.client.doRequest( "create_close", v, false )
  return err
}

func ( k *MogileKey ) Stream() (*http.Response, error) {
  path, err := k.Path()
  if( err != nil ) {
    return nil, err
  }
  request, requestErr := http.Get( path ) 
  if( requestErr != nil ) {
    return nil, requestErr
  }
  return request, nil
}

func ( k *MogileKey ) StoreReader( r io.Reader, contentType string ) error {
  fid, err := k.Domain.CreateOpen()
  if( err != nil ) {
    return err
  }
  path := fid.RandomPath()
  request, err := http.NewRequest( "PUT", path, r ) 
  if( err != nil ) {
    return err
  }
  request.Header.Add( "Content-Type", contentType )
  response, err = http.DefaultClient.Do( request ) 
  if( err != nil ) {
    return err
  }
  response.Body.Close()
  return k.CreateClose( fid, path, 0 );

}
func ( d *MogileDomain ) ListKeys( prefix string, after string , limit int ) ( *MogileKeyList, error ) {
  v := d.values()
  if( len( prefix ) > 0 ) {
    v.Add( "prefix", prefix )
  }
  if( len( after ) > 0 ) {
    v.Add( "after", after )
  }
  v.Add( "limit", strconv.Itoa( limit ) )
  data, err := d.client.doRequest( "list_keys", v, true )
  if( err != nil ) {
    return nil, err
  }
  numKeys, _ := strconv.Atoi( data.Get( "key_count" ) )
  rVal := make( []*MogileKey, numKeys )
  for i := 0; i < numKeys; i++ {
    var k MogileKey
    k.Key = data.Get( "key_" + strconv.Itoa( i + 1 ) )
    k.Domain = d
    rVal[i] = &k
  }
  var list MogileKeyList
  list.LastKey = data.Get( "next_after" )
  list.Keys = rVal
  return &list, err
}
