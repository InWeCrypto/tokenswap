package tokenswap

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/go-xorm/xorm"
	"github.com/inwecrypto/ethdb"
	"github.com/inwecrypto/ethgo"
	"github.com/inwecrypto/ethgo/erc20"
	ethkeystore "github.com/inwecrypto/ethgo/keystore"
	ethmath "github.com/inwecrypto/ethgo/math"
	ethrpc "github.com/inwecrypto/ethgo/rpc"
	ethtx "github.com/inwecrypto/ethgo/tx"
	"github.com/inwecrypto/gomq"
	"github.com/inwecrypto/neodb"
	neokeystore "github.com/inwecrypto/neogo/keystore"
)

const ETH_TNC_DECIAMLS = 18

// Monitor neo/eth tx event monitor
type Monitor struct {
	slf4go.Logger
	neomq       gomq.Consumer
	ethmq       gomq.Consumer
	tokenswapdb *xorm.Engine
	ethdb       *xorm.Engine
	neodb       *xorm.Engine
	tncOfNEO    string
	tncOfETH    string
	keyOfETH    *ethkeystore.Key
	keyOFNEO    *neokeystore.Key
	ethClient   *ethrpc.Client
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
		tncOfETH:    conf.GetString("eth.tnc", ""),
		tncOfNEO:    conf.GetString("neo.tnc", ""),
		keyOfETH:    ethKey,
		keyOFNEO:    neoKey,
		ethClient:   ethrpc.NewClient(conf.GetString("eth.node", "")),
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

func (monitor *Monitor) ethMonitor() {
	for {
		select {
		case message, ok := <-monitor.ethmq.Messages():
			if ok {
				if monitor.handleETHMessage(string(message.Key())) {
					monitor.ethmq.Commit(message)
				}
			}
		case err := <-monitor.ethmq.Errors():
			monitor.ErrorF("ethmq err, %s", err)
		}
	}
}

func (monitor *Monitor) neoMonitor() {
	for {
		select {
		case message, ok := <-monitor.neomq.Messages():
			if ok {
				if monitor.handleNEOMessage(string(message.Key())) {
					monitor.neomq.Commit(message)
				}
			}
		case err := <-monitor.neomq.Errors():
			monitor.ErrorF("neomq err, %s", err)
		}
	}
}

// Run .
func (monitor *Monitor) Run() {
	go monitor.ethMonitor()
	go monitor.neoMonitor()
}

func (monitor *Monitor) handleNEOMessage(txid string) bool {
	//	monitor.DebugF("handle neo tx %s", txid)

	neoTxs := make([]neodb.Tx, 0)

	err := monitor.neodb.Where(` "t_x" = ?`, txid).Find(&neoTxs)

	if err != nil {
		monitor.ErrorF("handle neo tx %s error, %s", txid, err)
		return false
	}

	for _, neoTx := range neoTxs {
		if neoTx.From == monitor.keyOFNEO.Address && neoTx.Asset == monitor.tncOfNEO {
			// complete order
			value, b := monitor.ParseNeoValueToCustomer(neoTx.Value)
			if !b {
				return false
			}

			order, err := monitor.getOrderByToAddress(neoTx.To, value, neoTx.CreateTime)

			if err != nil {
				monitor.ErrorF("handle neo tx %s error, %s", txid, err)
				return false
			}

			log := &Log{
				TX:         order.TX,
				CreateTime: time.Now(),
				Content:    fmt.Sprintf("release TNC to %s -- success", neoTx.To),
			}

			order.OutTx = neoTx.TX
			order.CompletedTime = time.Now()

			if err := monitor.insertLogAndUpdate(log, order, "out_tx", "completed_time"); err != nil {
				monitor.ErrorF("handle neo tx %s error, %s", txid, err)
				return false
			}

			return true

		} else if neoTx.To == monitor.keyOFNEO.Address && neoTx.Asset == monitor.tncOfNEO {
			value, b := monitor.ParseNeoValueToCustomer(neoTx.Value)
			if !b {
				return false
			}

			monitor.DebugF("checked tx  from:%s  to:%s  value:%s  asset:%s", neoTx.From, neoTx.To, value, monitor.tncOfNEO)

			order, err := monitor.getOrderByFromAddress(neoTx.From, value, neoTx.CreateTime)

			if err != nil {
				monitor.ErrorF("handle neo tx %s error, %s", txid, err)
				return false
			}

			order.InTx = neoTx.TX

			log := &Log{
				TX:         order.TX,
				CreateTime: time.Now(),
				Content:    fmt.Sprintf("recv TNC from %s -- success", neoTx.From),
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
	}

	return true
}

func (monitor *Monitor) ParseNeoValueToCustomer(value string) (string, bool) {
	x, b := ethmath.ParseUint64(value)
	if !b {
		monitor.ErrorF("handle tx error, parse  %s err", value)
		return "", false
	}

	f := float64(x) / 100000000.0

	return fmt.Sprint(f), true
}

func (monitor *Monitor) ParseEthValueToCustomer(value string) (string, bool) {
	x, b := ethmath.ParseBig256(value)
	if !b {
		monitor.ErrorF("handle tx error, parse  %s err", value)
		return "", false
	}

	d := ethgo.CustomerValue(x, big.NewInt(ETH_TNC_DECIAMLS))

	f, _ := d.Float64()

	return fmt.Sprint(f), true
}

func (monitor *Monitor) handleETHMessage(txid string) bool {
	//	monitor.DebugF("handle eth tx %s", txid)

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

	if ethTx.From == monitor.keyOfETH.Address && ethTx.Asset == monitor.tncOfETH {
		// complete order

		value, b := monitor.ParseEthValueToCustomer(ethTx.Value)
		if !b {
			return false
		}

		order, err := monitor.getOrderByToAddress(ethTx.To, value, ethTx.CreateTime)

		if err != nil {
			monitor.ErrorF("handle eth tx %s error, %s", txid, err)
			return false
		}

		log := &Log{
			TX:         order.TX,
			CreateTime: time.Now(),
			Content:    fmt.Sprintf("release TNC to %s -- success", ethTx.To),
		}

		order.OutTx = ethTx.TX
		order.CompletedTime = time.Now()

		if err := monitor.insertLogAndUpdate(log, order, "out_tx", "completed_time"); err != nil {
			monitor.ErrorF("handle eth tx %s error, %s", txid, err)
			return false
		}

		return true

	} else if ethTx.To == monitor.keyOfETH.Address && ethTx.Asset == monitor.tncOfETH {
		value, b := monitor.ParseEthValueToCustomer(ethTx.Value)
		if !b {
			return false
		}

		order, err := monitor.getOrderByFromAddress(ethTx.From, value, ethTx.CreateTime)

		if err != nil {
			monitor.ErrorF("handle eth tx %s error, %s", txid, err)
			return false
		}

		order.InTx = ethTx.TX

		log := &Log{
			TX:         order.TX,
			CreateTime: time.Now(),
			Content:    fmt.Sprintf("recv TNC from %s -- success", ethTx.From),
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

	_, err = session.Where(` "t_x" = ?`, order.TX).Cols(cls...).Update(order)

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
	amount, err := strconv.ParseFloat(order.Value, 64)

	if err != nil {
		monitor.ErrorF("ParseFloat err : %v", err)
		return err
	}

	transferValue := ethgo.FromCustomerValue(big.NewFloat(float64(amount)), big.NewInt(ETH_TNC_DECIAMLS))

	codes, err := erc20.Transfer(order.To, hex.EncodeToString(transferValue.Bytes()))
	if err != nil {
		monitor.ErrorF("get erc20.Transfer(%s,%f) code err: %v ", order.To, amount, err)
		return err
	}

	gasLimits := big.NewInt(61000)
	gasPrice := ethgo.NewValue(big.NewFloat(20), ethgo.Shannon)

	nonce, err := monitor.ethClient.Nonce(monitor.keyOfETH.Address)
	if err != nil {
		monitor.ErrorF("get Nonce   (%s,%f)  err: %v ", order.To, amount, err)
		return err
	}

	ntx := ethtx.NewTx(nonce, monitor.tncOfETH, nil, gasPrice, gasLimits, codes)
	err = ntx.Sign(monitor.keyOfETH.PrivateKey)
	if err != nil {
		monitor.ErrorF("Sign  (%s,%f)  err: %v ", order.To, amount, err)
		return err
	}

	rawtx, err := ntx.Encode()
	if err != nil {
		monitor.ErrorF("Encode  (%s,%f)  err: %v ", order.To, amount, err)
		return err
	}

	tx, err := monitor.ethClient.SendRawTransaction(rawtx)
	if err != nil {
		monitor.ErrorF("SendRawTransaction  (%s,%f)  err: %v ", order.To, amount, err)
		return err
	}

	monitor.InfoF("from : %s ", monitor.keyOfETH.Address)
	monitor.InfoF("to   : %s ", order.To)
	monitor.InfoF("value: %f ", amount)
	monitor.InfoF("tx   : %s ", tx)

	return nil
}

// getOrder .
func (monitor *Monitor) getOrderByToAddress(to, value string, createTime time.Time) (*Order, error) {

	order := new(Order)

	ok, err := monitor.tokenswapdb.Where(
		`"to" = ? and "value" = ? and "create_time" < ?`, to, value, createTime).Get(order)

	if err != nil {
		monitor.ErrorF("query to order(%s,%s) error, %s", to, value, err)
		return nil, err
	}

	if !ok {
		monitor.ErrorF("query to order(%s,%s) not found", to, value)
		return nil, nil
	}

	return order, nil
}

func (monitor *Monitor) getOrderByFromAddress(from, value string, createTime time.Time) (*Order, error) {

	order := new(Order)

	ok, err := monitor.tokenswapdb.Where(
		`"from" = ? and "value" = ? and "create_time" < ?`, from, value, createTime).Get(order)

	if err != nil {
		monitor.ErrorF("query from order(%s,%s) error, %s", from, value, err)
		return nil, err
	}

	if !ok {
		monitor.ErrorF("query from order(%s,%s) not found", from, value)
		return nil, nil
	}

	return order, nil
}
