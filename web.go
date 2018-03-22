package tokenswap

import (
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/gin-gonic/gin"
	"github.com/go-xorm/xorm"
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
	return server.engine.Run(server.laddr)
}

func (server *WebServer) makeRouters() {
	server.engine.POST("/trade", server.CreateOrder)
	server.engine.GET("/trade/:tx", server.GetOrder)
	server.engine.GET("/log/:tx", server.GetOrderLog)
}

func (server *WebServer) GetOrderLog(ctx *gin.Context) {
	ctx.Header("Access-Control-Allow-Origin", "*")
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
	from := ctx.Query("from")
	to := ctx.Query("to")
	value := ctx.Query("value")

	amount, err := strconv.ParseFloat(value, 64)
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
	r := float64(rand.Int63n(9999)+1) / 10000.0
	amount = amount + r

	order := Order{
		TX:         server.TXGenerate.Generate().String(),
		From:       from,
		To:         to,
		Value:      fmt.Sprint(amount),
		CreateTime: time.Now(),
	}

	_, err = server.db.Insert(order)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	res := make(map[string]string)
	res["TX"] = order.TX
	res["Value"] = order.Value
	res["Address"] = server.keyOFNEO.Address

	ctx.JSON(http.StatusOK, res)
}
