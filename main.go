package main

import (
	"log"
	"os"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/urfave/cli"
	"strings"
	"fmt"
		"github.com/kr/fs"
	"time"
	"path/filepath"
	"encoding/csv"
)

var replacer = strings.NewReplacer(
	" ", "_",
	"ä", "ae",
	"ö", "oe",
	"ü", "ue",
	"ß", "ss",
	"Ü", "Ue",
	"Ä", "Ae",
	"Ö", "Oe",
	"-", "_",
)

func main() {
	app := cli.NewApp()
	app.Name = "homee_csv_to_mysql"
	app.Usage = "Importiert homee CSV Dateien in MySQL"
	app.Commands = []cli.Command{
		{
			Name:    "import",
			Aliases: []string{"i"},
			Usage:   "Fuert den Import aus",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "input-dir",
					Value: "",
					Usage: "Stammverzeichnis fuer CSV Dateien",
				},
				cli.StringFlag{
					Name:  "mysql-dsn",
					Value: "",
					Usage: "MySQL connection DSN (vgl. https://github.com/Go-SQL-Driver/MySQL/#dsn-data-source-name)",
				},
				cli.BoolFlag{
					Name: "force-all",
					Usage: "Erzwinge import aller Daten, egal wie alt die Datei ist",
				},
			},
			Action: func(c *cli.Context) error {
				log.Println("Starte Import")
				mysqlConn, err := sql.Open("mysql", c.String("mysql-dsn"))
				defer mysqlConn.Close()

				log.Printf("Lese Dateien von %s", c.String("input-dir"))
				walker := fs.Walk(c.String("input-dir"))

				if err != nil {
					return err
				}

				for {
					if walker.Step() == false {
						break
					}

					path := walker.Path()

					// Wenn es ein Verzeichnis ist, dann wollen wir es nicht bearbeiten
					// Wenn es kein Verzeichnis ist und nicht auf .csv endet, dann wollen wir es nicht bearbeiten
					if walker.Stat().IsDir() || (!walker.Stat().IsDir() && !strings.HasSuffix(path, ".csv")) {
						continue
					}

					// Vorlage fuer die Mitternachtsbestimmung
					now := time.Now()

					// Heute morgen um 00:00
					interestingTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

					// Zeitstempel der Datei
					modTime := walker.Stat().ModTime()

					// Wenn der Zeitstempel nach der Tagesgrenze zum aktuellen Tag liegt verarbeiten wir die Datei
					if modTime.After(interestingTime) || c.Bool("force-all") {
						// Erstmal nur den Dateinamen ohne Pfad-Praefix
						fileName := filepath.Base(walker.Path())
						tableName := tableNameFromFileName(fileName)

						ensureTableExists(tableName, mysqlConn)
						file, err := os.Open(walker.Path())
						if err != nil {
							return err
						}

						csvReader := csv.NewReader(file)
						allRecords, err := csvReader.ReadAll()
						if err != nil {
							file.Close()
							return err
						}
						file.Close()

						generatedSQL := recordsToSQL(tableName, allRecords)
						_, err = mysqlConn.Exec(generatedSQL)

						if err != nil {
							return err
						}
					} else {
						log.Printf("Ueberspringe %s, da Zeitstempel zu alt %+v.", walker.Path(), walker.Stat().ModTime())
					}
				}
				return nil
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
func recordsToSQL(tableName string, allRecords [][]string) string {
	sqlLine := "INSERT IGNORE INTO " + tableName + " (homee_ts, val) VALUES "
	values := make([]string, len(allRecords))

	for i, row := range allRecords {
		if len(row) != 2 {
			log.Printf("found invalid record %+v, skipping", row)
			continue
		}
		values[i] = fmt.Sprintf("('%s', %s)", row[0], row[1])
	}
	return sqlLine + strings.Join(values, ",")
}

func ensureTableExists(s string, conn *sql.DB) {
	conn.Exec(
		`CREATE TABLE IF NOT EXISTS ` + s + ` (
  			homee_ts TIMESTAMP NOT NULL PRIMARY KEY,
  		  	val DECIMAL(10,2) NOT NULL DEFAULT 0.0);`)
}

func tableNameFromFileName(fileName string) string {
	return strings.ToLower(replacer.Replace(fileName[:strings.LastIndex(fileName, "_")]))
}
