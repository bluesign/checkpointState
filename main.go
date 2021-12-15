package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/onflow/atree"
	"github.com/onflow/cadence/runtime"

	"github.com/onflow/cadence/runtime/common"
	"github.com/onflow/cadence/runtime/interpreter"

	"github.com/onflow/flow-go/ledger/common/encoding"
	"github.com/onflow/flow-go/ledger/complete/mtrie/flattener"

	"database/sql"
	"log"

	badger "github.com/dgraph-io/badger/v3"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	//importCheckpointSlabs()
	decodeSlabsWithBadger()
}

const slabKeyLength = 9

func readUint16(buffer []byte, location int) (uint16, int) {
	value := binary.BigEndian.Uint16(buffer[location:])
	return value, location + 2
}
func readUint32(buffer []byte, location int) (uint32, int) {
	value := binary.BigEndian.Uint32(buffer[location:])
	return value, location + 4
}
func readUint64(buffer []byte, location int) (uint64, int) {
	value := binary.BigEndian.Uint64(buffer[location:])
	return value, location + 8
}

func PrepareTx(db *sql.DB, qry string) (tx *sql.Tx, s *sql.Stmt, e error) {
	if tx, e = db.Begin(); e != nil {
		panic(e)
	}

	if s, e = tx.Prepare(qry); e != nil {
		panic(e)
	}
	return
}

func decodeSlab(id atree.StorageID, data []byte) (atree.Slab, error) {
	return atree.DecodeSlab(
		id,
		data,
		interpreter.CBORDecMode,
		interpreter.DecodeStorable,
		interpreter.DecodeTypeInfo,
	)
}
func isSlabStorageKey(key string) bool {
	return len(key) == slabKeyLength && key[0] == '$'
}

func storageKeySlabStorageID(address atree.Address, key string) atree.StorageID {
	if !isSlabStorageKey(key) {
		return atree.StorageIDUndefined
	}
	var result atree.StorageID
	result.Address = address
	copy(result.Index[:], key[1:])
	return result
}

type BadgerLedger struct {
	db *badger.DB
}

func NewBadgerLedger(db *badger.DB) BadgerLedger {
	return BadgerLedger{db}
}

func (b BadgerLedger) retrieve(key []byte) func(*badger.Txn) ([]byte, error) {

	return func(tx *badger.Txn) ([]byte, error) {

		// retrieve the item from the key-value store
		item, err := tx.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, badger.ErrKeyNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("could not load data: %w", err)
		}

		v, err := item.ValueCopy(nil)

		if err != nil {
			return nil, fmt.Errorf("could not decode entity: %w", err)
		}

		return v, nil
	}
}

func (b BadgerLedger) GetValue(owner, key []byte) (value []byte, err error) {
	tx := b.db.NewTransaction(false)
	defer tx.Discard()
	fullPath := append([]byte{}, owner...)
	fullPath = append(fullPath, key...)

	v, err := b.retrieve(fullPath)(tx)
	if err != nil {
		fmt.Println(err)
	}
	return v, err
}

func (b BadgerLedger) SetValue(owner, key, value []byte) (err error) {
	return nil
}
func (b BadgerLedger) ValueExists(owner, key []byte) (exists bool, err error) {
	tx := b.db.NewTransaction(false)
	defer tx.Discard()
	fullPath := append([]byte{}, owner...)
	fullPath = append(fullPath, key...)
	fmt.Println(fullPath)
	_, err = b.retrieve(fullPath)(tx)

	if err != nil {
		return false, nil
	}
	return true, nil
}
func (b BadgerLedger) AllocateStorageIndex(owner []byte) (atree.StorageIndex, error) {
	return atree.StorageIndex{}, nil
}

func decode(s atree.Storable, storage atree.SlabStorage) (*interpreter.CompositeValue, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()
	storedValue := interpreter.StoredValue(s, storage)
	cs, _ := storedValue.(*interpreter.CompositeValue)
	return cs, nil
}
func decodeSlabsWithBadger() {
	db, err := badger.Open(badger.DefaultOptions("./badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var badgerLedger = NewBadgerLedger(db)

	storage := runtime.NewStorage(badgerLedger, func(f func(), report func(metrics runtime.Metrics, duration time.Duration)) {
		f()

	})

	r, _ := os.Open("./root.checkpoint")

	var bufReader io.Reader = bufio.NewReader(r)
	header := make([]byte, 4+8+2)
	_, err = io.ReadFull(bufReader, header)
	if err != nil {
		return
	}
	_, pos := readUint16(header, 0)
	_, pos = readUint16(header, pos)
	nodesCount, pos := readUint64(header, pos)
	_, _ = readUint16(header, pos)

	for i := uint64(1); i <= nodesCount; i++ {
		storableNode, err := flattener.ReadStorableNode(bufReader)

		if err != nil {
			return
		}
		if storableNode.Path != nil {

			payload, _ := encoding.DecodePayload(storableNode.EncPayload)
			//payloadDecoded, _ := hex.DecodeString(payload.Value.String())
			//fmt.Println(payloadDecoded)
			//storedData, version := interpreter.StripMagic(payloadDecoded)
			addr := common.BytesToAddress(payload.Key.KeyParts[0].Value)
			//controller := common.BytesToAddress(payload.Key.KeyParts[1].Value)
			path := payload.Key.KeyParts[2].Value
			pathN := bytes.Replace(path, []byte{0x1f}, []byte("/"), 100)

			address := payload.Key.KeyParts[0].Value

			fullPath := append([]byte{}, address...)
			fullPath = append(fullPath, path...)

			var treeaddress atree.Address
			copy(treeaddress[:], payload.Key.KeyParts[0].Value)

			//sid := storageKeySlabStorageID(treeaddress, string(path))
			if strings.Contains(string(path), "storage\x1f") {
				fmt.Println(addr, string(pathN))
				decoded := storage.ReadValue(nil, addr, string(path))

				fmt.Println(decoded)

				continue
			}

		}
		if i%50000 == 0 {
			fmt.Printf("%d/%d %d\n", i/50000, nodesCount/50000, i*100/nodesCount)
		}

	}

}

func importCheckpointSlabs() {
	r, _ := os.Open("./root.checkpoint")

	var bufReader io.Reader = bufio.NewReader(r)
	header := make([]byte, 4+8+2)
	_, err := io.ReadFull(bufReader, header)
	if err != nil {
		return
	}
	_, pos := readUint16(header, 0)
	_, pos = readUint16(header, pos)
	nodesCount, pos := readUint64(header, pos)
	_, _ = readUint16(header, pos)
	db, err := badger.Open(badger.DefaultOptions("./badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	for i := uint64(1); i <= nodesCount; i++ {
		storableNode, err := flattener.ReadStorableNode(bufReader)

		if err != nil {
			return
		}
		if storableNode.Path != nil {

			payload, _ := encoding.DecodePayload(storableNode.EncPayload)
			payloadDecoded, _ := hex.DecodeString(payload.Value.String())
			path := payload.Key.KeyParts[2].Value
			address := payload.Key.KeyParts[0].Value
			path = payload.Key.KeyParts[2].Value

			fullPath := append([]byte{}, address...)
			fullPath = append(fullPath, path...)

			err := db.Update(func(txn *badger.Txn) error {
				err := txn.Set([]byte(fullPath), payloadDecoded)
				return err
			})
			if err != nil {
				fmt.Println(err)
			}

		}
		if i%50000 == 0 {
			fmt.Printf("%d/%d %d\n", i/50000, nodesCount/50000, i*100/nodesCount)
		}

	}

}
