package tokenswap

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/gin-gonic/gin"
	"github.com/go-xorm/xorm"
)

type WebServer struct {
	slf4go.Logger
	engine     *gin.Engine
	db         *xorm.Engine
	laddr      string
	TXGenerate *snowflake.Node
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

	node, err := snowflake.NewNode(1)
	if err != nil {
		return nil, err
	}

	server := &WebServer{
		engine:     engine,
		Logger:     slf4go.Get("tokenswap"),
		laddr:      conf.GetString("webladdr", ":8000"),
		db:         tokenswapdb,
		TXGenerate: node,
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
}

func (server *WebServer) GetOrder(ctx *gin.Context) {
	tx := ctx.Param("tx")
	order := &Order{}
	server.db.Where(` "tx" = ?`, tx).Get(order)

	ctx.JSON(http.StatusOK, order)
}

func (server *WebServer) CreateOrder(ctx *gin.Context) {
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

	order := Order{
		TX:         server.TXGenerate.Generate().String(),
		From:       from,
		To:         to,
		Value:      value,
		CreateTime: time.Now(),
	}

	_, err = server.db.Insert(order)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
}
