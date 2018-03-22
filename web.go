package tokenswap

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/go-xorm/xorm"
	"github.com/inwecrypto/ethgo"
	ethkeystore "github.com/inwecrypto/ethgo/keystore"
	neokeystore "github.com/inwecrypto/neogo/keystore"
)

type WebServer struct {
	slf4go.Logger
	engine     *gin.Engine
	db         *xorm.Engine
	laddr      string
	TXGenerate *snowflake.Node
	keyOfETH   *ethkeystore.Key
	keyOFNEO   *neokeystore.Key
}

func NewWebServer(conf *config.Config) (*WebServer, error) {
	tokenswapdb, err := createEngine(conf, "tokenswapdb")

	if err != nil {
		return nil, fmt.Errorf("create tokenswap db engine error %s", err)
	}

	if !conf.GetBool("meshnode.debug", true) {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()
	engine.Use(gin.Recovery())

	if conf.GetBool("meshnode.debug", true) {
		engine.Use(gin.Logger())
	}

	ethKey, err := readETHKeyStore(conf, "eth.keystore", conf.GetString("eth.keystorepassword", ""))

	if err != nil {
		return nil, fmt.Errorf("create neo db engine error %s", err)
	}

	neoKey, err := readNEOKeyStore(conf, "neo.keystore", conf.GetString("neo.keystorepassword", ""))

	if err != nil {
		return nil, fmt.Errorf("create neo db engine error %s", err)
	}

	node, err := snowflake.NewNode(1)
	if err != nil {
		return nil, err
	}

	server := &WebServer{
		engine:     engine,
		Logger:     slf4go.Get("tokenswap"),
		laddr:      conf.GetString("tokenswap.webladdr", ":8000"),
		db:         tokenswapdb,
		TXGenerate: node,
		keyOfETH:   ethKey,
		keyOFNEO:   neoKey,
	}

	server.makeRouters()

	return server, nil
}

// Run run http service
func (server *WebServer) Run() error {

	// 允许跨域
	config := cors.DefaultConfig()
	config.AllowHeaders = append(config.AllowHeaders, "Accept")
	config.AllowHeaders = append(config.AllowHeaders, "Authorization")
	config.AllowHeaders = append(config.AllowHeaders, "X-Requested-With")
	config.AllowHeaders = append(config.AllowHeaders, "ct")
	config.AllowHeaders = append(config.AllowHeaders, "Lang")
	config.AllowAllOrigins = true
	server.engine.Use(cors.New(config))

	return server.engine.Run(server.laddr)
}

func (server *WebServer) makeRouters() {
	server.engine.POST("/trade", server.CreateOrder)
	server.engine.GET("/trade/:tx", server.GetOrder)
	server.engine.GET("/log/:tx", server.GetOrderLog)
}

func (server *WebServer) GetOrderLog(ctx *gin.Context) {
	ctx.Header("Access-Control-Allow-Origin", "*")
	ctx.Header("access-control-allow-headers", "Content-Type, Accept, Authorization, X-Requested-With, ct, Origin, X_Requested_With, Lang")

	tx := ctx.Param("tx")
	logs := make([]Log, 0)
	err := server.db.Where(` "t_x" = ?`, tx).Find(&logs)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, logs)
}

func (server *WebServer) GetOrder(ctx *gin.Context) {
	ctx.Header("Access-Control-Allow-Origin", "*")
	ctx.Header("access-control-allow-headers", "Content-Type, Accept, Authorization, X-Requested-With, ct, Origin, X_Requested_With, Lang")

	tx := ctx.Param("tx")
	order := &Order{}
	_, err := server.db.Where(` "t_x" = ?`, tx).Get(order)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, order)
}

func (server *WebServer) CreateOrder(ctx *gin.Context) {
	ctx.Header("Access-Control-Allow-Origin", "*")
	ctx.Header("access-control-allow-headers", "Content-Type, Accept, Authorization, X-Requested-With, ct, Origin, X_Requested_With, Lang")

	from := ctx.Query("from")
	to := ctx.Query("to")
	value := ctx.Query("value")

	amount, err := strconv.Atoi(value)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// TODO 参数校验
	if from == "" || to == "" || amount <= 0 {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// 添加随机数,防止重放
	r := rand.Intn(9999) + 1
	amountres := float64(amount) + float64(r)/10000.0

	amount = amount*10000 + r

	ethValue := ethgo.FromCustomerValue(big.NewFloat(float64(amount)), big.NewInt(14))

	order := Order{
		TX:         server.TXGenerate.Generate().String(),
		From:       from,
		To:         to,
		Value:      "0x" + hex.EncodeToString(ethValue.Bytes()),
		CreateTime: time.Now(),
	}

	_, err = server.db.Insert(order)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	res := make(map[string]string)
	res["TX"] = order.TX
	res["Value"] = fmt.Sprint(amountres)
	res["Address"] = server.keyOFNEO.Address

	ctx.JSON(http.StatusOK, res)
}
