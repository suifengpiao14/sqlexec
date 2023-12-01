package sqlexec_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlexec"
)

func GetExecutorSQL() (executorSql *sqlexec.ExecutorSQL) {
	dbConfig := sqlexec.DBConfig{
		DSN: `root:1b03f8b486908bbe34ca2f4a4b91bd1c@mysql(127.0.0.1:3306)/curdservice?charset=utf8&timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true`,
	}
	sshConfig := &sqlexec.SSHConfig{
		Address:        "120.24.156.100:2221",
		User:           "root",
		PriviteKeyFile: "C:\\Users\\Admin\\.ssh\\id_rsa",
	}

	executorSql = sqlexec.NewExecutorSQL(dbConfig, sshConfig)
	return executorSql

}

func TestSshMysql(t *testing.T) {
	sshConfig := sqlexec.SSHConfig{
		Address:  "ip:port",
		User:     "username",
		Password: "",
	}
	dbDSN := "user:password@tcp(127.0.0.1:3306)/ad?charset=utf8&timeout=1s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true"

	db, err := sshConfig.Tunnel(dbDSN)
	require.NoError(t, err)
	sql := "select count(*) from ad.advertise;"
	var count int64
	err = db.QueryRow(sql).Scan(&count)
	require.NoError(t, err)
	fmt.Println(count)

}
