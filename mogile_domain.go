package gomog

import( 
  "strconv"
  "net/url"
  "fmt"
  )

type MogileDomain struct {
  Client *MogileClient
  Domain string
}


func ( c *MogileClient ) GetDomains() ( [](*MogileDomain), error ){
  data, err := c.doRequest( "get_domains", url.Values{}, true )
  numDomains, _ := strconv.Atoi( data.Get( "domains" ) )
  domains := make( []*MogileDomain, numDomains )
  for i := 0; i < numDomains; i++ {
    var d MogileDomain
    var s string
    s = data.Get( "domain" + strconv.Itoa(i + 1) )
    d.Domain = s
    d.Client = c
    domains[i] = &d
  }
  return domains, err
}

func ( c *MogileClient ) Domain( d string ) *MogileDomain {
  var domain MogileDomain
  domain.Domain = d
  domain.Client = c
  return &domain
}
func ( d *MogileDomain ) Create() ( error ) {
  _, err := d.doRequest( "create_domain", d.values(), false )
  return err
}
func ( d *MogileDomain ) Delete() ( error ) {
  _, err := d.doRequest( "delete_domain", d.values(), false )
  return err
}
func ( d *MogileDomain ) values() url.Values {
  v :=  url.Values{}
  v.Add( "domain", d.Domain )
  return v
}
func ( d *MogileDomain ) CreateOpen() (*MogileFid, error ) {
  data, err := d.doRequest( "create_open", d.values(), false )
  if( err != nil ) {
    return nil, err
  }
  var f MogileFid 
  devCount := data.Get( "dev_count" )
  if len( devCount ) > 0 {
    fmt.Print( "++++++++++++WTF" )
  } else {
    f.DeviceId, _ = strconv.Atoi( data.Get( "devid" ) )
    f.FileId, _ = strconv.Atoi( data.Get( "fid" ) )
    f.Paths = make( []string, 1 )
    f.Paths[0] = data.Get( "path" )
  }
  return &f, err

}
func ( d *MogileDomain ) Exists() (bool, error ) {
  domains, err := d.Client.GetDomains()
  if( err != nil ) {
    return false, err
  }
  for _, domain := range( domains ) {
    if( domain.Domain == d.Domain ) {
      return true, nil
    }
  }
  return false, nil
}
func (d *MogileDomain ) doRequest( cmd string, args url.Values, isIdempotent bool ) ( url.Values, error ) {
  return d.Client.doRequest( cmd, args, isIdempotent )
}
func ( d *MogileDomain ) CreateClose( fid *MogileFid, path string, size int64, key string ) error {
  v := d.values()
  v.Add( "fid", strconv.Itoa( fid.FileId ) )
  v.Add( "devid", strconv.Itoa( fid.DeviceId ) )
  v.Add( "path", path )
  v.Add( "size", strconv.FormatInt( size, 10 ) )
  v.Add( "key", key )
  _, err := d.doRequest( "create_close", v, false )
  return err
}
