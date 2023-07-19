package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"log"
	"time"

	"github.com/360EntSecGroup-Skylar/excelize/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)
type User struct {
	Id string `json:"id"`
	UserName string `json:"name"`
	FirstName string `json:"firstName"`
	LastName string `json:"lastName"`
}

func main() {
	fmt.Println("Welcome to xlsx program! :: Naim Biswas")
	importXLSX()
}

func importXLSX()  {

	
	xlsxFilePath, _ := filepath.Abs("./file/employeeSheets.xlsx")
	xlsxFile, err := excelize.OpenFile(xlsxFilePath)
	if err != nil {
		log.Fatal(err)
	}
	
	db := dbConnection()

	startTime := time.Now()
	for _, sheetName  := range xlsxFile.GetSheetList() {
		fmt.Println("sheet Name:",sheetName)
		timeForEachSheet := time.Now()
		rows, err := xlsxFile.GetRows(sheetName)
		// columns, err := xlsxFile.GetCols(sheetName)

		if err != nil {
			log.Println("Error reading sheet rows:", err)
			continue
		}
		colNames  := rows[0]
		rowValues := rows[1:]
		
		var jsonData []map[string]interface{}
		fmt.Printf("Xlsx Process Started for=================================================================::%v and time:: %v",sheetName, time.Now())
		fmt.Println()
		for _, rowValue := range rowValues {
			data :=  make(map[string]interface{})

			for idx, rV := range rowValue {
				if colNames[idx] != "" {
					data[colNames[idx]] = rV
				 }
			}
			jsonData = append(jsonData, data)
		}
		insertRecords(db, "excelData", jsonData)
		jsonBytes, err := json.MarshalIndent(jsonData, "", "  ")
		if err != nil {
			log.Fatal(err)
		}
		outputFile, err := os.Create("./convertedFile/"+sheetName+".json")
		defer outputFile.Close()
		
		// Write JSON data to a file
		_, err = outputFile.Write(jsonBytes)
		fmt.Println("Conversion completed. JSON data is stored in",sheetName+".json", time.Since(timeForEachSheet))
	}
	elapsedTime := time.Since(startTime)
	// Print the elapsed time
	fmt.Println("Insertion completed in:", elapsedTime)
}


func insertRecords(db *mongo.Database, collectionName string, data []map[string]interface{}) error {
	collection := db.Collection(collectionName)
	var documents []interface{}
	for _, record := range data {
		if len(record) != 0 {
			documents = append(documents, record)
		} 
	}
	_, err := collection.InsertMany(context.Background(), documents)
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println("Records Inserted Into Database,collection:", collectionName, "RecordLength:", len(documents))
	return nil
}

func getAllData(db *mongo.Database, collectionName string) ([]interface{})   {
	
	cur, err := db.Collection(collectionName).Find(context.Background(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		var data []interface{}
		err := cur.Decode(&data)
		if err != nil {
			log.Fatal(err)
		}
		return data
	}
	return []interface{}{}
}

func dbConnection() (*mongo.Database) {
	mongoURL := "mongodb+srv://NaimBiswas:yUhrDIMBoOAPKV2J@ecom.9wdffud.mongodb.net/eCom?retryWrites=true&w=majority"
	// collectionName := "excelData"
	dbName := "eCom"

	clientOptions := options.Client().ApplyURI(mongoURL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal("error:",err)
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	db := client.Database(dbName)
	return db
}