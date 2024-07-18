package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"net/http"
	"os"
	"strings"
)

func main() {
	val, present := os.LookupEnv("OPENSEARCH_INITIAL_ADMIN_PASSWORD")
	if !present {
		fmt.Println("password not found")
		os.Exit(1)
	}
	fmt.Println(val)

	client, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				},
				Addresses: []string{"https://localhost:9200"},
				Username:  "admin", // For testing only. Don't store credentials in code.
				Password:  val,
			},
		},
	)

	if err != nil {
		fmt.Println("cannot initialize", err)
		os.Exit(1)
	}

	fmt.Println(client.Client)
	fmt.Println("client created")

	index := "movies"
	ctx := context.Background()

	// Run this program again to get resource_already_exists_exception response
	indexCreateResp, err := client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{Index: index})
	if err != nil {
		fmt.Println(err.Error())
	}
	respAsJson, err := json.MarshalIndent(indexCreateResp, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Create Index:\n%s\n", respAsJson)

	indexDeleteResp, err := client.Indices.Delete(ctx,
		opensearchapi.IndicesDeleteReq{
			Indices: []string{"games"},
			Params: opensearchapi.IndicesDeleteParams{
				IgnoreUnavailable: opensearchapi.ToPointer(true),
			},
		},
	)

	if err != nil {
		fmt.Println(err.Error())
	}
	respAsJson, err = json.MarshalIndent(indexDeleteResp, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Delete Index, Ignore Unavailable true:\n%s\n", respAsJson)

	// with ignore unavailable as false, it returns index_not_found_exception response
	indexDeleteResp, err = client.Indices.Delete(ctx,
		opensearchapi.IndicesDeleteReq{
			Indices: []string{"games"},
			Params: opensearchapi.IndicesDeleteParams{
				IgnoreUnavailable: opensearchapi.ToPointer(false),
			},
		},
	)

	if err != nil {
		fmt.Println(err.Error())
	}
	respAsJson, err = json.MarshalIndent(indexDeleteResp, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Delete Index, Ignore Unavailable false:\n%s\n", respAsJson)

	docCreateResp, err := client.Document.Create(
		ctx,
		opensearchapi.DocumentCreateReq{
			Index:      "movies",
			DocumentID: "1",
			Body:       strings.NewReader(`{"title": "Beauty and the Beast", "year": 1991 }`),
		},
	)
	if err != nil {
		fmt.Println(err.Error())
	}
	respAsJson, err = json.MarshalIndent(docCreateResp, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Create Doc:\n%s\n", respAsJson)

	// Run this program again to get version_conflict_engine_exception response
	docCreateResp, err = client.Document.Create(
		ctx,
		opensearchapi.DocumentCreateReq{
			Index:      "movies",
			DocumentID: "2",
			Body:       strings.NewReader(`{"title": "Beauty and the Beast - Live Action", "year": 2017 }`),
		},
	)
	if err != nil {
		fmt.Println(err.Error())
	}
	respAsJson, err = json.MarshalIndent(docCreateResp, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Create Doc:\n%s\n", respAsJson)

	docDelResp, err := client.Document.Delete(ctx, opensearchapi.DocumentDeleteReq{Index: "movies", DocumentID: "1"})
	if err != nil {
		fmt.Println(err)
	}
	respAsJson, err = json.MarshalIndent(docDelResp, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Del Doc:\n%s\n", respAsJson)

	// In case of error in delete, the server gives response in different format when compared to errors in other API like create index, delete index, etc.
	docDelResp, err = client.Document.Delete(ctx, opensearchapi.DocumentDeleteReq{Index: "movies", DocumentID: "3"})
	if err != nil {
		fmt.Println(err)
	}
	respAsJson, err = json.MarshalIndent(docDelResp, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Del Doc:\n%s\n", respAsJson)

}
