package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	grideye "github.com/synerex/proto_grideye"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// datastore provider provides Datastore Service.

type DataStore interface {
	store(str string)
}

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	mu              sync.Mutex
	version         = "0.01"
	baseDir         = flag.String("baseDir", "default", "Relative path")
	dataDir         string
	sxServerAddress string
	ds              DataStore
)

func init() {
	var err error
	dataDir, err = os.Getwd()
	if err != nil {
		fmt.Printf("Can't obtain current wd")
	}
	if dataDir == "default" {
		dataDir = filepath.ToSlash(dataDir) + "/store"
	} else {
		dataDir = *baseDir
	}
	ds = &FileSystemDataStore{
		storeDir: dataDir,
	}
}

type FileSystemDataStore struct {
	storeDir  string
	storeFile *os.File
	todayStr  string
}

// open file with today info
func (fs *FileSystemDataStore) store(str string) {
	const layout = "2006-01-02"
	day := time.Now()
	todayStr := day.Format(layout) + ".csv"
	if fs.todayStr != "" && fs.todayStr != todayStr {
		fs.storeFile.Close()
		fs.storeFile = nil
	}
	if fs.storeFile == nil {
		_, er := os.Stat(fs.storeDir)
		if er != nil { // create dir
			er = os.MkdirAll(fs.storeDir, 0777)
			if er != nil {
				fmt.Printf("Can't make dir '%s'.", fs.storeDir)
				return
			}
		}
		fs.todayStr = todayStr
		file, err := os.OpenFile(filepath.FromSlash(fs.storeDir+"/"+todayStr), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("Can't open file '%s'", todayStr)
			return
		}
		fs.storeFile = file
	}
	fs.storeFile.WriteString(str + "\n")
}

func supplyGridEyeCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {

	ge := &grideye.GridEye{}

	err := proto.Unmarshal(sp.Cdata.Entity, ge)
	if err == nil { // get GridEye
		ts0 := ptypes.TimestampString(ge.Ts)
		ld := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%d", ts0, ge.DeviceId, ge.Hostname, ge.Location, ge.Mac, ge.Ip, ge.Seq)
		for _, ev := range ge.Data {
			ts := ptypes.TimestampString(ev.Ts)
			line := fmt.Sprintf("%s,%s,%s,%s,%d,%v", ld, ts, ev.Typ, ev.Id, ev.Seq, ev.Temps)
			ds.store(line)
		}
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock()
	if client.Client != nil {
		client.Client = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.Client == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.Client = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server\n", sxServerAddress)
	}
	mu.Unlock()
}

func subscribeGridEyeSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyGridEyeCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("GridEye-Store(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	channelTypes := []uint32{pbase.GRIDEYE_SVC}

	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "GridEyeStore", channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connecting SynerexServer")
	}

	geClient := sxutil.NewSXServiceClient(client, pbase.GRIDEYE_SVC, "{Client:GridEyeStore}")

	wg.Add(1)
	log.Print("Subscribe Supply")
	go subscribeGridEyeSupply(geClient)

	wg.Wait()

}
