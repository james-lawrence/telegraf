package postgresql

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	// register in driver.
	_ "github.com/jackc/pgx/stdlib"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type Postgresql struct {
	Service
	Databases        []string
	IgnoredDatabases []string
	OrderedColumns   []string
	AllColumns       []string
}

var ignoredColumns = map[string]bool{"stats_reset": true}

var sampleConfig = `
  ## specify address via a url matching:
  ##   postgres://[pqgotest[:password]]@localhost[/dbname]\
  ##       ?sslmode=[disable|verify-ca|verify-full]
  ## or a simple string:
  ##   host=localhost user=pqotest password=... sslmode=... dbname=app_production
  ##
  ## All connection parameters are optional.
  ##
  ## Without the dbname parameter, the driver will default to a database
  ## with the same name as the user. This dbname is just for instantiating a
  ## connection with the server and doesn't restrict the databases we are trying
  ## to grab metrics for.
  ##
  address = "host=localhost user=postgres sslmode=disable"

	## connection configuration.

	## maxidle - specify the number of idle connections to maintain. see sql.DB for details.
	## default is 1. setting to 0 to force disconnect after use.
	max_idle = 1

	## maxopen - specify the maximum number of connections to maintain. see sql.DB for details.
	## default is 1.
	max_open = 1

	## maxlifetime - specify the maximum lifetime of a connection.
	## default is forever (0s)
	max_lifetime = "0s"

  ## A  list of databases to explicitly ignore.  If not specified, metrics for all
  ## databases are gathered.  Do NOT use with the 'databases' option.
  # ignored_databases = ["postgres", "template0", "template1"]

  ## A list of databases to pull metrics about. If not specified, metrics for all
  ## databases are gathered.  Do NOT use with the 'ignored_databases' option.
  # databases = ["app_production", "testing"]
`

func (p *Postgresql) SampleConfig() string {
	return sampleConfig
}

func (p *Postgresql) Description() string {
	return "Read metrics from one or many postgresql servers"
}

func (p *Postgresql) IgnoredColumns() map[string]bool {
	return ignoredColumns
}

func (p *Postgresql) Gather(acc telegraf.Accumulator) error {
	var (
		err   error
		query string
	)

	if len(p.Databases) == 0 && len(p.IgnoredDatabases) == 0 {
		query = `SELECT * FROM pg_stat_database`
	} else if len(p.IgnoredDatabases) != 0 {
		query = fmt.Sprintf(`SELECT * FROM pg_stat_database WHERE datname NOT IN ('%s')`,
			strings.Join(p.IgnoredDatabases, "','"))
	} else {
		query = fmt.Sprintf(`SELECT * FROM pg_stat_database WHERE datname IN ('%s')`,
			strings.Join(p.Databases, "','"))
	}

	rows, err := p.DB.Query(query)
	if err != nil {
		return err
	}

	defer rows.Close()

	// grab the column information from the result
	if p.OrderedColumns, err = rows.Columns(); err != nil {
		return err
	}

	p.AllColumns = make([]string, len(p.OrderedColumns))
	copy(p.AllColumns, p.OrderedColumns)

	for rows.Next() {
		err = p.accRow(rows, acc)
		if err != nil {
			return err
		}
	}

	query = `SELECT * FROM pg_stat_bgwriter`

	bg_writer_row, err := p.DB.Query(query)
	if err != nil {
		return err
	}

	defer bg_writer_row.Close()

	// grab the column information from the result
	if p.OrderedColumns, err = bg_writer_row.Columns(); err != nil {
		return err
	}

	for _, v := range p.OrderedColumns {
		p.AllColumns = append(p.AllColumns, v)
	}

	for bg_writer_row.Next() {
		err = p.accRow(bg_writer_row, acc)
		if err != nil {
			return err
		}
	}
	sort.Strings(p.AllColumns)
	return bg_writer_row.Err()
}

type scanner interface {
	Scan(dest ...interface{}) error
}

func (p *Postgresql) accRow(row scanner, acc telegraf.Accumulator) error {
	var columnVars []interface{}
	var dbname bytes.Buffer

	// this is where we'll store the column name with its *interface{}
	columnMap := make(map[string]*interface{})

	for _, column := range p.OrderedColumns {
		columnMap[column] = new(interface{})
	}

	// populate the array of interface{} with the pointers in the right order
	for i := 0; i < len(columnMap); i++ {
		columnVars = append(columnVars, columnMap[p.OrderedColumns[i]])
	}

	// deconstruct array of variables and send to Scan
	err := row.Scan(columnVars...)

	if err != nil {
		return err
	}
	if columnMap["datname"] != nil {
		// extract the database name from the column map
		dbname.WriteString((*columnMap["datname"]).(string))
	} else {
		dbname.WriteString("postgres")
	}

	var tagAddress string
	tagAddress, err = p.SanitizedAddress()
	if err != nil {
		return err
	}

	tags := map[string]string{"server": tagAddress, "db": dbname.String()}

	fields := make(map[string]interface{})
	for col, val := range columnMap {
		_, ignore := ignoredColumns[col]
		if !ignore {
			fields[col] = *val
		}
	}
	acc.AddFields("postgresql", fields, tags)

	return nil
}

func init() {
	inputs.Add("postgresql", func() telegraf.Input {
		return &Postgresql{
			Service: Service{
				MaxIdle: 1,
				MaxOpen: 1,
				MaxLifetime: internal.Duration{
					Duration: 0,
				},
			},
		}
	})
}
