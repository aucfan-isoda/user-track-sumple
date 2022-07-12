package main

import (
	"fmt"
	"github.com/aucfan-yotsuya/gomod/db"
	"time"
	"context"
)

func Copy() {
	var (
		d *db.DB
		err error
	)
	d = db.New()
	d.NewTarget().NewConn(&db.DbConnOpt{
		Driver:  "mysql",
		Dsn:     "root:@tcp(aucfan-user-track-staging.cluster-ro-cnceg4b5jglv.ap-northeast-1.rds.amazonaws.com:3306)/pro?timeout=10s&charset=utf8mb4&interpolateParams=true&parseTime=true&loc=Asia%2fTokyo",
		Timeout: 10 * time.Second,
	})
	if err = d.GetTarget(0).Conn.Ping(); err != nil {
		fmt.Println(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	a,err := d.GetTarget(0).Select(ctx,"select * from user_track limit 100;")

	if err != nil{
		fmt.Println(err)
	}

	fmt.Println(a)
}
