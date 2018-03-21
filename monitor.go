package tokenswap

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/go-xorm/xorm"
	"github.com/inwecrypto/ethdb"
	ethkeystore "github.com/inwecrypto/ethgo/keystore"
	"github.com/inwecrypto/gomq"
	"github.com/inwecrypto/neodb"
	neokeystore "github.com/inwecrypto/neogo/keystore"
)

// Monitor neo/eth tx event monitor
type Monitor struct {
	slf4go.Logger
	neomq       gomq.Consumer
	ethmq       gomq.Consumer
	tokenswapdb *xorm.Engine
	ethdb       *xorm.Engine
	neodb       *xorm.Engine
	neolockaddr string
	ethlockaddr string
	tncOfNEO    string
	tncOfETH    string
	keyOfETH    *ethkeystore.Key
	keyOFNEO    *neokeystore.Key
}

// NewMonitor create new monitor
func NewMonitor(conf *config.Config, neomq, ethmq gomq.Consumer) (*Monitor, error) {

	tokenswapdb, err := createEngine(conf, "tokenswapdb")

	if err != nil {
		return nil, fmt.Errorf("create tokenswap db engine error %s", err)
	}

	ethdb, err := createEngine(conf, "ethdb")

	if err != nil {
		return nil, fmt.Errorf("create eth db engine error %s", err)
	}

	neodb, err := createEngine(conf, "neodb")

	if err != nil {
		return nil, fmt.Errorf("create neo db engine error %s", err)
	}

	ethKey, err := readETHKeyStore(conf, "eth.keystore", conf.GetString("eth.keystorepassword", ""))

	if err != nil {
		return nil, fmt.Errorf("create neo db engine error %s", err)
	}

	neoKey, err := readNEOKeyStore(conf, "neo.keystore", conf.GetString("neo.keystorepassword", ""))

	if err != nil {
		return nil, fmt.Errorf("create neo db engine error %s", err)
	}

	return &Monitor{
		Logger:      slf4go.Get("monitor"),
		neomq:       neomq,
		ethmq:       ethmq,
		tokenswapdb: tokenswapdb,
		ethdb:       ethdb,
		neodb:       neodb,
		neolockaddr: conf.GetString("tokenswap.neolockaddr", "xxxx"),
		ethlockaddr: conf.GetString("tokenswap.ethlockaddr", "xxxx"),
		tncOfETH:    conf.GetString("eth.tnc", ""),
		tncOfNEO:    conf.GetString("neo.tnc", ""),
		keyOfETH:    ethKey,
		keyOFNEO:    neoKey,
	}, nil
}

func readETHKeyStore(conf *config.Config, name string, password string) (*ethkeystore.Key, error) {
	data, err := json.Marshal(conf.Get(name))

	if err != nil {
		return nil, err
	}

	return ethkeystore.ReadKeyStore(data, password)
}

func readNEOKeyStore(conf *config.Config, name string, password string) (*neokeystore.Key, error) {
	data, err := json.Marshal(conf.Get(name))

	if err != nil {
		return nil, err
	}

	return neokeystore.ReadKeyStore(data, password)
}

func createEngine(conf *config.Config, name string) (*xorm.Engine, error) {
	driver := conf.GetString(fmt.Sprintf("%s.driver", name), "postgres")
	datasource := conf.GetString(fmt.Sprintf("%s.datasource", name), "")

	return xorm.NewEngine(driver, datasource)
}

// NEOAddress .
func (monitor *Monitor) NEOAddress() string {
	return monitor.keyOFNEO.Address
}

// Run .
func (monitor *Monitor) Run() {
	for {
		select {
		case message, ok := <-monitor.neomq.Messages():
			if ok {
				if monitor.handleNEOMessage(string(message.Key())) {
					monitor.neomq.Commit(message)
				}
			}
		case message, ok := <-monitor.ethmq.Messages():
			if ok {
				if monitor.handleETHMessage(string(message.Key())) {
					monitor.neomq.Commit(message)
				}
			}
		case err := <-monitor.neomq.Errors():
			monitor.ErrorF("neomq err, %s", err)
		case err := <-monitor.ethmq.Errors():
			monitor.ErrorF("ethmq err, %s", err)
		}
	}
}

func (monitor *Monitor) handleNEOMessage(txid string) bool {
	monitor.DebugF("handle neo tx %s", txid)

	neoTx := new(neodb.Tx)

	ok, err := monitor.neodb.Where("t_x = ?", txid).Get(neoTx)

	if err != nil {
		monitor.ErrorF("handle neo tx %s error, %s", txid, err)
		return false
	}

	if !ok {
		monitor.WarnF("handle neo tx %s -- not found", txid)
		return true
	}

	if neoTx.From == monitor.neolockaddr && neoTx.Asset == monitor.tncOfNEO {
		// complete order

		order, err := monitor.getOrderByToAddress(neoTx.To, neoTx.Value, neoTx.CreateTime)

		if err != nil {
			monitor.ErrorF("handle neo tx %s error, %s", txid, err)
			return false
		}

		log := &Log{
			TX:         order.TX,
			CreateTime: time.Now(),
			Content:    fmt.Sprintf("release neo to %s -- success", neoTx.To),
		}

		order.OutTx = neoTx.TX

		if err := monitor.insertLogAndUpdate(log, order, "out_tx"); err != nil {
			monitor.ErrorF("handle neo tx %s error, %s", txid, err)
			return false
		}

		return true

	} else if neoTx.To == monitor.neolockaddr && neoTx.Asset == monitor.tncOfNEO {
		order, err := monitor.getOrderByFromAddress(neoTx.From, neoTx.Value, neoTx.CreateTime)

		if err != nil {
			monitor.ErrorF("handle neo tx %s error, %s", txid, err)
			return false
		}

		order.InTx = neoTx.TX

		log := &Log{
			TX:         order.TX,
			CreateTime: time.Now(),
			Content:    fmt.Sprintf("recv neo from %s -- success", neoTx.From),
		}

		if err := monitor.insertLogAndUpdate(log, order, "in_tx"); err != nil {
			monitor.ErrorF("handle neo tx %s error, %s", txid, err)
			return false
		}

		if err := monitor.sendETH(order); err != nil {
			monitor.ErrorF("handle neo tx %s -- send neo error %s", txid, err)
			return false
		}

		return true
	}

	return true
}

func (monitor *Monitor) handleETHMessage(txid string) bool {
	monitor.DebugF("handle eth tx %s", txid)

	ethTx := new(ethdb.TableTx)

	ok, err := monitor.ethdb.Where("t_x = ?", txid).Get(ethTx)

	if err != nil {
		monitor.ErrorF("handle eth tx %s error, %s", txid, err)
		return false
	}

	if !ok {
		monitor.WarnF("handle eth tx %s -- not found", txid)
		return true
	}

	if ethTx.From == monitor.ethlockaddr && ethTx.Asset == monitor.tncOfETH {
		// complete order

		order, err := monitor.getOrderByToAddress(ethTx.To, ethTx.Value, ethTx.CreateTime)

		if err != nil {
			monitor.ErrorF("handle eth tx %s error, %s", txid, err)
			return false
		}

		log := &Log{
			TX:         order.TX,
			CreateTime: time.Now(),
			Content:    fmt.Sprintf("release eth to %s -- success", ethTx.To),
		}

		order.OutTx = ethTx.TX
		order.CompletedTime = time.Now()

		if err := monitor.insertLogAndUpdate(log, order, "out_tx"); err != nil {
			monitor.ErrorF("handle eth tx %s error, %s", txid, err)
			return false
		}

		return true

	} else if ethTx.To == monitor.ethlockaddr && ethTx.Asset == monitor.tncOfETH {
		order, err := monitor.getOrderByFromAddress(ethTx.From, ethTx.Value, ethTx.CreateTime)

		if err != nil {
			monitor.ErrorF("handle eth tx %s error, %s", txid, err)
			return false
		}

		order.InTx = ethTx.TX

		log := &Log{
			TX:         order.TX,
			CreateTime: time.Now(),
			Content:    fmt.Sprintf("recv eth from %s -- success", ethTx.From),
		}

		if err := monitor.insertLogAndUpdate(log, order, "in_tx"); err != nil {
			monitor.ErrorF("handle eth tx %s error, %s", txid, err)
			return false
		}

		if err := monitor.sendNEO(order); err != nil {
			monitor.ErrorF("handle eth tx %s -- send neo error %s", txid, err)
			return false
		}

		return true
	}

	return true
}

func (monitor *Monitor) insertLogAndUpdate(log *Log, order *Order, cls ...string) error {

	session := monitor.tokenswapdb.NewSession()

	defer session.Close()

	_, err := session.InsertOne(log)

	if err != nil {
		session.Rollback()
		monitor.ErrorF("insert order(%s,%s,%s) log error, %s", order.From, order.To, order.Value, err)
		return err
	}

	_, err = session.Where("t_x = ?", order.TX).Cols(cls...).Update(order)

	if err != nil {
		session.Rollback()
		monitor.ErrorF("update order(%s,%s,%s) error, %s", order.From, order.To, order.Value, err)
		return err
	}

	return nil
}

func (monitor *Monitor) sendNEO(order *Order) error {
	return fmt.Errorf("Not implement")
}

func (monitor *Monitor) sendETH(order *Order) error {
	return nil
}

// getOrder .
func (monitor *Monitor) getOrderByToAddress(to, value string, createTime time.Time) (*Order, error) {

	order := new(Order)

	ok, err := monitor.tokenswapdb.Where(
		`"to" = ? and "value" = ? and "create_time" < `, to, value, createTime).Get(order)

	if err != nil {
		monitor.ErrorF("query order(,%s,%s) error, %s", to, value, err)
		return nil, err
	}

	if !ok {
		monitor.ErrorF("query order(,%s,%s) not found", to, value)
		return nil, nil
	}

	return order, nil
}

func (monitor *Monitor) getOrderByFromAddress(from, value string, createTime time.Time) (*Order, error) {

	order := new(Order)

	ok, err := monitor.tokenswapdb.Where(
		`"from" = ? and "value" = ? and "create_time" < `, from, value, createTime).Get(order)

	if err != nil {
		monitor.ErrorF("query order(%s,,%s) error, %s", from, value, err)
		return nil, err
	}

	if !ok {
		monitor.ErrorF("query order(%s,,%s) not found", from, value)
		return nil, nil
	}

	return order, nil
}
