package gomog

import( "net"
  "errors"
  "sync"
  "strconv"
  "strings"
  "time"
  "net/url"
  "math/rand"
  "regexp"
  )

var errRegex *regexp.Regexp = regexp.MustCompile( "^ERR\\s+(\\w+)\\s*([^\\r\\n]*)" )
var successRegex *regexp.Regexp = regexp.MustCompile( "^OK\\s+\\d*\\s*(\\S*)" )
var newlineRegex *regexp.Regexp = regexp.MustCompile( "\r?\n\\z" )

var SocketError error = errors.New( "Socket error!" )
var SocketReadError error = errors.New( "Socket read error!" )
var TruncatedRequestError error = errors.New( "Request sent was unexpectedly truncated" )
var InvalidResponseError error = errors.New( "Response was not valid!" )
var NotImplementedError error = errors.New( "That command is not implemented yet" )

type MogileHost struct {
  Name string
  AlternateMask *string
  AlternateIp *net.IPAddr
  IpAddress *net.IPAddr
  Id int
  Status string
  Port int
  GetPort *int
}
type MogileFid struct {
  DeviceId int
  Class string
  FileId int
  DevCount int
  Size int64
  Key string
  Domain string
  Paths []string
}
type MogileDevice struct {
  mbAsof string
  HostId int
  SpaceUsed int
  SpaceFree int
  SpaceTotal int
  Id int
  Status string
  State string
  Weight int
  Utilization string
  RejectBadMd5 bool
}
type MogileClient struct {
  sync.Mutex
  Hosts []*net.TCPAddr
  Timeout time.Duration
  Socket *net.TCPConn
  LastError string
  LastErrorMessage string
}
func New( hosts []string ) ( *MogileClient, error ) {
  addresses := make( []*net.TCPAddr, 0 )
  for i, _ := range( hosts ) {
    address, err := net.ResolveTCPAddr( "tcp", hosts[i] );
    if( err != nil ) {
      return nil, errors.New( "Could not read as address: " + hosts[i] );
    }
    addresses = append( addresses, address )
  }
  var client MogileClient
  client.Hosts = addresses
  client.Timeout = time.Duration( 5 )
  err := client.setupConnection()
  if( err != nil ) {
    return nil, err 
  }
  return &client, nil
}

func makeRequest( cmd string, args url.Values ) string {
  return cmd + " " + args.Encode() + "\r\n"
}

func ( c *MogileClient ) doRequest( cmd string, args url.Values, isIdempotent bool ) ( url.Values, error ) {
  request := makeRequest( cmd, args )
  
  c.Lock()
  defer c.Unlock()
  var data []byte = make( []byte, 512 )
  deadlineError := c.Socket.SetDeadline( time.Now().Add( time.Second * 30 ) );
  if( deadlineError != nil ) {
    err = c.setupConnection();
    if( err != nil ) {
      return nil, err
    }
    deadlineError = c.Socket.SetDeadline( time.Now().Add( time.Second * 30 ) );
    if( deadlineError != nil ) {
      return nil, deadlineError
    }
  }
  for {
    bytesSent, err := c.Socket.Write( []byte(request) )
    if( err != nil ) {
      err = c.setupConnection();
      if( err != nil ) {
        return nil, err
      }
      bytesSent, err = c.Socket.Write( []byte(request) )
      if( err != nil ) {
        return nil, errors.New( "Socket error on write after retry!" )
      }
    }
    if( err != nil || bytesSent != len( request )) {
      c.Shutdown()
      if( err == nil ) {
        return nil, SocketError
      } else {
        return nil, err
      }
    }
    c.Socket.SetReadDeadline( time.Now().Add( time.Second * 5 ) );
    bytes, readError := c.Socket.Read( data )
    data = data[0:bytes]
    if( readError != nil ) {
      return nil, readError
    }
    if( newlineRegex.Match( data ) ) {
      break;
    }
    if( bytes > 0 ) {
      return nil, errors.New( "Bad mogile server response: " + string(data) );
    }
    if( !isIdempotent ) {
      return nil, errors.New( "Unexpected EOF after: " + request );
    }
  }
  return c.parseResponse( string( data ) )
}
func (c *MogileClient ) Shutdown() {
//TODO: Write me
}


func (c *MogileClient ) parseResponse( line string ) (url.Values, error) {
  if( errRegex.MatchString( line ) ) {
    results := errRegex.FindAllStringSubmatch( line, -1 )
    c.LastError = results[0][1]
    c.LastErrorMessage = results[0][2]
    return nil, errorCode( c.LastError )
  }
  matches := successRegex.FindStringSubmatch( line )
  if ( matches != nil ) {
    if( len( matches ) > 1 ) {
      return unescapeResponse( matches[1] ), nil
    } else {
      return nil, nil
    }
  }
  return nil, InvalidResponseError
}
func errorCode( err string  ) error {
  return errors.New( "Mogile error: " + err );
}

/*
func (s Socket ) isReadable( timeout int ) bool {
  for {
    t0 := Time.now
    found = IO.select( [s], nil, nil, timeout )
    return true if found && found[0]
    timeleft -= (Time.now = t0 )
    if( timeleft < 0 ) {
      peer = s.peer_name
      Shutdown();
      raise "Unreadable socket!"
      break
    }
    Shutdown();
  }
  return false;
}
*/

func (c *MogileClient ) setupConnection() error {
  order := rand.Perm( len( c.Hosts ) )
  if( c.Socket != nil ) {
    c.Socket.Close() 
  }
  for i, _ := range( order ) {
    conn, err := net.DialTCP( "tcp4", nil, c.Hosts[i] )
    if( err != nil ) {
      continue
    }

    c.Socket = conn
    conn.SetKeepAlive( true )
    return nil
  }
  return errors.New( "Connection was not setup" )
}

var escapeRegex *regexp.Regexp = regexp.MustCompile( "/%([a-f0-9][a-f0-9])/i" )
func unescapeResponse( s string ) url.Values {
    rv := url.Values{}
    dataLines := strings.Split( s, "&" )
    for _, part := range( dataLines ) {
      if len( part ) == 0 {
        continue 
      }
      data := strings.SplitN( part, "=", 2 )
      rv.Add( data[0], data[1] )
    }
    return rv
}

func ( c *MogileClient ) Noop() ( error ) {
  _, err := c.doRequest( "noop", url.Values{}, true )
  return err
}
func ( c *MogileClient ) Sleep() ( error ) {
  _, err := c.doRequest( "sleep", url.Values{}, true )
  return err
}

func ( c *MogileClient ) FileDebug() ( url.Values, error ){
  return c.doRequest( "file_debug", url.Values{}, true )
}


func ( c *MogileClient ) GetHosts() ( []*MogileHost, error ){
  data, err := c.doRequest( "get_hosts", url.Values{}, true )
  numHosts, _ := strconv.Atoi( data.Get( "hosts" ) )
  rVal := make( []*MogileHost, numHosts )
  for i := 0; i < numHosts; i++ {
    base := "host" + strconv.Itoa( i + 1 ) + "_"
    var h MogileHost
    h.Name = data.Get( base + "hostname" )
    h.Status = data.Get( base + "status" )
    altMask := data.Get( base + "altmask" )
    if( len( altMask ) > 0 ) {
      h.AlternateMask = &altMask
    }
    altIp := data.Get( base + "altip" )
    if( len( altIp ) > 0 ) {
      h.AlternateIp, _ = net.ResolveIPAddr( "ip", altIp )
    }
    hostIp := data.Get( base + "hostip" )
    if( len( hostIp ) > 0 ) {
      h.IpAddress, _ = net.ResolveIPAddr( "ip", hostIp )
    }
    h.Id, _ = strconv.Atoi( data.Get( "hostid" ) )
    h.Port, _ = strconv.Atoi( data.Get( "http_port" ) )
    getPort := data.Get( "http_get_port" )
    if( len( getPort ) > 0 ) {
      p, _ := strconv.Atoi( getPort )
      h.GetPort = &p
    }
    rVal[i] = &h
  }
  return rVal, err
}
func ( c *MogileClient ) GetDevices() ( []*MogileDevice, error ){
  data, err := c.doRequest( "get_devices", url.Values{}, true )
  numDevices, _ := strconv.Atoi( data.Get( "devices" ) )
  rVal := make( []*MogileDevice, numDevices )
  for i := 0; i < numDevices; i++  {
    var d MogileDevice
    base := "dev" + strconv.Itoa( i + 1 ) + "_"
    d.mbAsof = data.Get( base + "mb_asof" )
    d.HostId, _ = strconv.Atoi( data.Get( base + "hostid" ) )
    d.SpaceUsed, _ = strconv.Atoi( data.Get( base + "mb_used" ) )
    d.SpaceFree, _ = strconv.Atoi( data.Get( base + "mb_free" ) )
    d.SpaceTotal, _ = strconv.Atoi( data.Get( base + "mb_total" ) )
    d.Id, _ = strconv.Atoi( data.Get( base + "devid" ) )
    d.Status = data.Get( base+"status" )
    d.State = data.Get( base + "observed_state" )
    d.Weight, _ = strconv.Atoi( data.Get( base + "weight" ) )
    d.Utilization = data.Get( base + "utilization" )
    md5, _ := strconv.Atoi( data.Get( base + "reject_bad_md5" ) )
    d.RejectBadMd5 =  ( md5 == 1 )
    rVal[i] = &d

  }
  return rVal, err
}
func ( c *MogileClient ) CreateDevice() ( error ) {
  _, err := c.doRequest( "create_device", url.Values{}, false )
  return err
}
func ( c *MogileClient ) CreateClass() ( error ) {
  _, err := c.doRequest( "create_class", url.Values{}, false )
  return err
}
func ( c *MogileClient ) UpdateClass() ( error ) {
  _, err := c.doRequest( "update_class", url.Values{}, false )
  return err
}
func ( c *MogileClient ) DeleteClass() ( error ) {
  _, err := c.doRequest( "delete_class", url.Values{}, false )
  return err
}
func ( c *MogileClient ) CreateHost() ( error ) {
  _, err := c.doRequest( "create_host", url.Values{}, false )
  return err
}
func ( c *MogileClient ) UpdateHost() ( error ) {
  _, err := c.doRequest( "update_host", url.Values{}, false )
  return err
}
func ( c *MogileClient ) DeleteHost() ( error ) {
  _, err := c.doRequest( "delete_host", url.Values{}, false )
  return err
}

func ( c *MogileClient ) SetState() ( error ) {
  _, err := c.doRequest( "set_state", url.Values{}, false )
  return err
}
func ( c *MogileClient ) SetWeight() ( error ) {
  _, err := c.doRequest( "set_weight", url.Values{}, false )
  return err
}
func ( c *MogileClient ) ReplicateNow() ( error ) {
  _, err := c.doRequest( "replice_now", url.Values{}, false )
  return err
}

func ( c *MogileClient ) Stats() ( *string, error ) {
  return nil, NotImplementedError
}


func ( f *MogileFid ) RandomPath() string {
  return f.Paths[ rand.Int31n( int32( len( f.Paths ) ) )  ];
}

