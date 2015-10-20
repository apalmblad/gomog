package gomog
import "testing"
import "fmt"
import "os"
import "io"
import "sync"

func TestBlah( t *testing.T ) {
  var err error
  var  client *MogileClient
  s := []string{"127.0.0.1:7001"}
  client, err = New( s )
  var wg  sync.WaitGroup

  if( err != nil ) {
    fmt.Println( "Mogile connection error:", err, "hosts were: ", s )
    os.Exit( 1 );
    panic( err.Error() )
  }
  key := client.Domain( "test" ).Key( "new_test_file" )
  reader, writer := io.Pipe()
  wg.Add( 2 )
  go func() {
    defer wg.Done()
    writer.Write( []byte("Test") )
    writer.Close()
    //resizeChannel <- nil
  }()

  go func() {
    defer wg.Done()
    err = key.StoreReader( reader, "text/plain" )
    reader.Close()
  }()
  wg.Wait()
  if( err != nil ) {
    fmt.Println( "Store error:", err )
    t.FailNow()
  }

  _, e := key.FileInfo()
  if( e != nil ) {
    fmt.Println( "ERROR FOR FILE_INFO", e )  
    t.FailNow()
  }

}

