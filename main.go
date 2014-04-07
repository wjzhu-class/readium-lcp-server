package main

import (
	"crypto/tls"
	"database/sql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"path/filepath"
	"runtime"
	//"github.com/readium/readium-lcp-server/epub"
	//"github.com/readium/readium-lcp-server/crypto"
	//"github.com/readium/readium-lcp-server/pack"
	"github.com/kylelemons/go-gypsy/yaml"
	"github.com/readium/readium-lcp-server/index"
	"github.com/readium/readium-lcp-server/license"
	"github.com/readium/readium-lcp-server/server"
	"github.com/readium/readium-lcp-server/storage"
	//"archive/zip"
	"os"
	"strings"
	//"fmt"
)

func dbFromURI(uri string) (string, string) {
	parts := strings.Split(uri, "://")
	return parts[0], parts[1]
}

func main() {
	var host, port, dbURI, storagePath, certFile, privKeyFile string
	var readonly bool = false
	var err error

	if host = os.Getenv("HOST"); host == "" {
		host, err = os.Hostname()
		if err != nil {
			panic(err)
		}
	}

	config_file := "config.yaml"

	config, err := yaml.ReadFile(config_file)
	if err != nil {
		panic("can't read config file : " + config_file)
	}

	readonly = os.Getenv("READONLY") != ""

	if port = os.Getenv("PORT"); port == "" {
		port = "8989"
	}

	dbURI, _ = config.Get("database")
	if dbURI == "" {
		if dbURI = os.Getenv("DB"); dbURI == "" {
			dbURI = "sqlite3://file:test.sqlite?cache=shared&mode=rwc"
		}
	}

	storagePath, _ = config.Get("storage.filesystem.storage")
	if storagePath == "" {
		if storagePath = os.Getenv("STORAGE"); storagePath == "" {
			storagePath = "files"
		}
	}

	certFile, _ = config.Get("certificate.cert")
	privKeyFile, _ = config.Get("certificate.private_key")

	if certFile == "" {
		if certFile = os.Getenv("CERT"); certFile == "" {
			panic("Must specify a certificate")
		}
	}

	if privKeyFile == "" {
		if privKeyFile = os.Getenv("PRIVATE_KEY"); privKeyFile == "" {
			panic("Must specify a private key")
		}
	}

	cert, err := tls.LoadX509KeyPair(certFile, privKeyFile)
	if err != nil {
		panic(err)
	}

	driver, cnxn := dbFromURI(dbURI)
	db, err := sql.Open(driver, cnxn)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("PRAGMA journal_mode = WAL")
	if err != nil {
		panic(err)
	}
	idx, err := index.Open(db)
	if err != nil {
		panic(err)
	}

	lst, err := license.NewSqlStore(db)
	if err != nil {
		panic(err)
	}

	os.Mkdir(storagePath, os.ModePerm) //ignore the error, the folder can already exist
	store := storage.NewFileSystem(storagePath, "http://"+host+":"+port+"/files")
	if err != nil {
		panic(err)
	}

	_, file, _, _ := runtime.Caller(0)
	here := filepath.Dir(file)
	static := filepath.Join(here, "/static")

	s := server.New(":"+port, static, readonly, &idx, &store, &lst, &cert)
	s.ListenAndServe()
	//zipfile, err := zip.OpenReader("test/samples/sample.epub")
	//if err != nil {
	//panic(err)
	//}
	//ep, err := epub.Read(zipfile.Reader)
	//if err != nil {
	//panic(err)
	//}
	//fmt.Println(ep)

	//ep, k, err := pack.Do(ep)
	//fmt.Println(k)
	//w, err := os.Create("test/output.epub")
	//if err != nil {
	//panic(err)
	//}
	//err = ep.Write(w)
	//defer w.Close()
	//if err != nil {
	//panic(err)
	//}

	//zipfile, err = zip.OpenReader("test/output.epub")
	//if err != nil {
	//panic(err)
	//}
	//ep, err = epub.Read(zipfile.Reader)
	//if err != nil {
	//panic(err)
	//}
	//ep, err = pack.Undo(k, ep)
	//if err != nil {
	//panic(err)
	//}
	//w, err = os.Create("test/decrypted.epub")
	//if err != nil {
	//panic(err)
	//}
	//err = ep.Write(w)
	//defer w.Close()

	//log.Printf("Bytes read: %d", offset)
	//zipReader, err := zip.NewReader(bytes.NewReader(b), int64(len(b)))
	//if err != nil {
	//panic(err)
	//}

}
