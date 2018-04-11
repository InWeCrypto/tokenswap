package tokenswap

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/btcsuite/btcutil/base58"
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
	"github.com/inwecrypto/neogo/nep5"
	neorpc "github.com/inwecrypto/neogo/rpc"
	neotx "github.com/inwecrypto/neogo/tx"
)

const ETH_TNC_DECIAMLS = 8

// Monitor neo/eth tx event monitor
type Monitor struct {
	slf4go.Logger
	neomq         gomq.Consumer
	ethmq         gomq.Consumer
	tokenswapdb   *xorm.Engine
	ethdb         *xorm.Engine
	neodb         *xorm.Engine
	tncOfNEO      string
	tncOfETH      string
	ETHKeyAddress string
	NEOKeyAddress string
	ethClient     *ethrpc.Client
	neoClient     *neorpc.Client
	config        *config.Config
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
		Logger:        slf4go.Get("monitor"),
		neomq:         neomq,
		ethmq:         ethmq,
		tokenswapdb:   tokenswapdb,
		ethdb:         ethdb,
		neodb:         neodb,
		tncOfETH:      conf.GetString("eth.tnc", ""),
		tncOfNEO:      conf.GetString("neo.tnc", ""),
		ETHKeyAddress: ethKey.Address,
		NEOKeyAddress: neoKey.Address,
		ethClient:     ethrpc.NewClient(conf.GetString("eth.node", "")),
		neoClient:     neorpc.NewClient(conf.GetString("neo.node", "")),
		config:        conf,
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
	return monitor.NEOKeyAddress
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
	//	monitor.DebugF("tokenswap handle neo tx %s", txid)

	neoTxs := make([]neodb.Tx, 0)

	err := monitor.neodb.Where(` "t_x" = ?`, txid).Find(&neoTxs)

	if err != nil {
		monitor.ErrorF("handle neo tx %s error, %s", txid, err)
		return false
	}

	for _, neoTx := range neoTxs {
		if neoTx.From == monitor.NEOKeyAddress && neoTx.Asset == monitor.tncOfNEO {
			monitor.DebugF("checked neo tx  from:%s  to:%s  value:%s  asset:%s", neoTx.From, neoTx.To, neoTx.Value, monitor.tncOfNEO)

			order, err := monitor.getOrderByToAddress(neoTx.To, neoTx.Value, neoTx.CreateTime, ` "in_tx" != '' and  "out_tx" = '' `)

			if err != nil {
				monitor.ErrorF("handle order in neo tx %s error, %s", txid, err)
				return false
			}

			log := &Log{
				TX:         order.TX,
				CreateTime: time.Now(),
				Content:    fmt.Sprintf("release TNC to %s success, tx: %s", neoTx.To, txid),
			}

			order.OutTx = neoTx.TX
			order.CompletedTime = time.Now()

			if err := monitor.insertLogAndUpdate(log, order, "out_tx", "completed_time"); err != nil {
				monitor.ErrorF("handle neo tx %s error, %s", txid, err)
				return false
			}

			return true

		} else if neoTx.To == monitor.NEOKeyAddress && neoTx.Asset == monitor.tncOfNEO {

			monitor.DebugF("checked neo tx  from:%s  to:%s  value:%s  asset:%s", neoTx.From, neoTx.To, neoTx.Value, monitor.tncOfNEO)

			order, err := monitor.getOrderByFromAddress(neoTx.From, neoTx.Value, neoTx.CreateTime, ` "in_tx" = '' and  "out_tx" = '' `)

			if err != nil {
				monitor.ErrorF("handle order in neo tx %s error, %s", txid, err)
				return false
			}

			order.InTx = neoTx.TX

			log := &Log{
				TX:         order.TX,
				CreateTime: time.Now(),
				Content:    fmt.Sprintf("recv TNC from %s success, tx: %s", neoTx.From, txid),
			}

			if err := monitor.insertLogAndUpdate(log, order, "in_tx"); err != nil {
				monitor.ErrorF("handle neo tx %s error, %s", txid, err)
				return false
			}

			if err := monitor.sendETH(order); err != nil {
				monitor.ErrorF("handle neo tx %s -- send TNC error %s", txid, err)
				return false
			}

			return true
		}
	}

	return true
}

func (monitor *Monitor) parseEthValue(value string) string {
	bigValue, _ := ethmath.ParseBig256(value)
	v := bigValue.Int64()

	return fmt.Sprint(v)
}

func (monitor *Monitor) handleETHMessage(txid string) bool {
	//	monitor.DebugF("tokenswap handle eth tx %s", txid)

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

	if ethTx.From == monitor.ETHKeyAddress && ethTx.Asset == monitor.tncOfETH {
		// complete order
		value := monitor.parseEthValue(ethTx.Value)

		monitor.DebugF("checked eth tx  from:%s  to:%s  value:%s  asset:%s", ethTx.From, ethTx.To, value, monitor.tncOfETH)

		order, err := monitor.getOrderByToAddress(ethTx.To, value, ethTx.CreateTime, ` "in_tx" != '' and  "out_tx" = '' `)

		if err != nil {
			monitor.ErrorF("handle order in eth tx %s error, %s", txid, err)
			return false
		}

		log := &Log{
			TX:         order.TX,
			CreateTime: time.Now(),
			Content:    fmt.Sprintf("release TNC to %s success, tx: %s", ethTx.To, txid),
		}

		order.OutTx = ethTx.TX
		order.CompletedTime = time.Now()

		if err := monitor.insertLogAndUpdate(log, order, "out_tx", "completed_time"); err != nil {
			monitor.ErrorF("handle eth tx %s error, %s", txid, err)
			return false
		}

		return true

	} else if ethTx.To == monitor.ETHKeyAddress && ethTx.Asset == monitor.tncOfETH {

		value := monitor.parseEthValue(ethTx.Value)

		monitor.DebugF("checked eth tx  from:%s  to:%s  value:%s  asset:%s", ethTx.From, ethTx.To, value, monitor.tncOfETH)

		order, err := monitor.getOrderByFromAddress(ethTx.From, value, ethTx.CreateTime, ` "in_tx" = '' and  "out_tx" = '' `)

		if err != nil {
			monitor.ErrorF("handle order in eth tx  %s error, %s", txid, err)
			return false
		}

		order.InTx = ethTx.TX

		log := &Log{
			TX:         order.TX,
			CreateTime: time.Now(),
			Content:    fmt.Sprintf("recv TNC from %s success, tx: %s", ethTx.From, txid),
		}

		if err := monitor.insertLogAndUpdate(log, order, "in_tx"); err != nil {
			monitor.ErrorF("handle eth tx %s error, %s", txid, err)
			return false
		}

		if err := monitor.sendNEO(order); err != nil {
			monitor.ErrorF("handle eth tx %s -- send TNC error %s", txid, err)
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

	amount, b := ethmath.ParseUint64(order.Value)
	if !b {
		monitor.ErrorF("ParseUint64  %s  err  ", order.Value)
		return errors.New("ParseUint64 err")
	}

	transferValue := big.NewInt(int64(amount))

	key, err := readNEOKeyStore(monitor.config, "neo.keystore", monitor.config.GetString("neo.keystorepassword", ""))

	if err != nil {
		monitor.ErrorF("read neo key  error %s", err)
		return err
	}

	from := ToInvocationAddress(key.Address)

	to := ToInvocationAddress(order.To)

	scriptHash, err := hex.DecodeString(strings.TrimPrefix(monitor.tncOfNEO, "0x"))
	if err != nil {
		monitor.ErrorF("DecodeString  error %s", err)
		return err
	}

	scriptHash = reverseBytes(scriptHash)

	bytesOfFrom, err := hex.DecodeString(from)
	if err != nil {
		monitor.ErrorF("DecodeString  error %s", err)
		return err
	}

	bytesOfFrom = reverseBytes(bytesOfFrom)

	bytesOfTo, err := hex.DecodeString(to)
	if err != nil {
		monitor.ErrorF("DecodeString  error %s", err)
		return err
	}

	bytesOfTo = reverseBytes(bytesOfTo)

	script, err := nep5.Transfer(scriptHash, bytesOfFrom, bytesOfTo, transferValue)

	if err != nil {
		monitor.ErrorF("Transfer  error:%s,  to:%s  value: %v", err, order.To, order.Value)
		return err
	}

	nonce, _ := time.Now().MarshalBinary()

	tx := neotx.NewInvocationTx(script, 0, bytesOfFrom, nonce)

	rawtx, txId, err := tx.Tx().Sign(key.PrivateKey)

	if err != nil {
		monitor.ErrorF("Sign  error:%s,   to:%s  value: %v", err, order.To, order.Value)
		return err
	}

	status, err := monitor.neoClient.SendRawTransaction(rawtx)

	if err != nil || !status {
		monitor.ErrorF("SendRawTransaction  error:%s,  to:%s  value: %v", err, order.To, order.Value)
		return err
	}

	monitor.InfoF("from : %s ", monitor.NEOKeyAddress)
	monitor.InfoF("to   : %s ", order.To)
	monitor.InfoF("value: %v ", order.Value)
	monitor.InfoF("tx   : %s ", txId)

	return nil
}

func (monitor *Monitor) sendETH(order *Order) error {

	amount, b := ethmath.ParseUint64(order.Value)
	if !b {
		monitor.ErrorF("ParseUint64  %s  err  ", order.Value)
		return errors.New("ParseUint64 err")
	}

	transferValue := big.NewInt(int64(amount))

	codes, err := erc20.Transfer(order.To, hex.EncodeToString(transferValue.Bytes()))
	if err != nil {
		monitor.ErrorF("get erc20.Transfer(%s,%v) code err: %v ", order.To, order.Value, err)
		return err
	}

	gasLimits := big.NewInt(65000)
	gasPrice := ethgo.NewValue(big.NewFloat(20), ethgo.Shannon)

	nonce, err := monitor.ethClient.Nonce(monitor.ETHKeyAddress)
	if err != nil {
		monitor.ErrorF("get Nonce   (%s,%v)  err: %v ", order.To, order.Value, err)
		return err
	}

	ethKey, err := readETHKeyStore(monitor.config, "eth.keystore", monitor.config.GetString("eth.keystorepassword", ""))

	if err != nil {
		monitor.ErrorF("read eth key error %s", err)
		return err
	}

	ntx := ethtx.NewTx(nonce, monitor.tncOfETH, nil, gasPrice, gasLimits, codes)
	err = ntx.Sign(ethKey.PrivateKey)
	if err != nil {
		monitor.ErrorF("Sign  (%s,%v)  err: %v ", order.To, order.Value, err)
		return err
	}

	rawtx, err := ntx.Encode()
	if err != nil {
		monitor.ErrorF("Encode  (%s,%v)  err: %v ", order.To, order.Value, err)
		return err
	}

	monitor.DebugF("from new address: %s,%s,%s", monitor.ETHKeyAddress, ethKey.Address, ethKey.PrivateKey)

	tx, err := monitor.ethClient.SendRawTransaction(rawtx)
	if err != nil {
		monitor.ErrorF("SendRawTransaction  (%s,%v)  err: %v ", order.To, order.Value, err)
		return err
	}

	monitor.InfoF("from : %s ", monitor.ETHKeyAddress)
	monitor.InfoF("to   : %s ", order.To)
	monitor.InfoF("value: %v ", order.Value)
	monitor.InfoF("tx   : %s ", tx)

	return nil
}

// getOrder .
func (monitor *Monitor) getOrderByToAddress(to, value string, createTime time.Time, where string) (*Order, error) {

	order := new(Order)

	if where != "" {
		where = " and " + where
	}

	ok, err := monitor.tokenswapdb.Where(
		`"to" = ? and "value" = ? and "create_time" < ?  `+where, to, value, createTime).Get(order)

	if err != nil {
		monitor.ErrorF("query to order(%s,%s,%s) error, %s", to, value, where, err)
		return nil, err
	}

	if !ok {
		monitor.ErrorF("query to order(%s,%s,%s) not found", to, value, where)
		return nil, errors.New("not found")
	}

	return order, nil
}

func (monitor *Monitor) getOrderByFromAddress(from, value string, createTime time.Time, where string) (*Order, error) {

	order := new(Order)

	if where != "" {
		where = " and " + where
	}

	ok, err := monitor.tokenswapdb.Where(
		`"from" = ? and "value" = ? and "create_time" < ? `+where, from, value, createTime).Get(order)

	if err != nil {
		monitor.ErrorF("query from order(%s,%s,%s) error, %s", from, value, where, err)
		return nil, err
	}

	if !ok {
		monitor.ErrorF("query from order(%s,%s,%s) not found", from, value, where)
		return nil, errors.New("not found")
	}

	return order, nil
}

// ToInvocationAddress neo wallet address to invocation address
func ToInvocationAddress(address string) string {
	bytesOfAddress, _ := decodeAddress(address)

	return hex.EncodeToString(reverseBytes(bytesOfAddress))
}

func reverseBytes(s []byte) []byte {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}

	return s
}

func decodeAddress(address string) ([]byte, error) {

	result, _, err := base58.CheckDecode(address)

	if err != nil {
		return nil, err
	}

	return result[0:20], nil
}
