package main

import (
	"bytes"
	"flag"
	"github.com/dave/jennifer/jen"
	"github.com/knq/snaker"
	"github.com/pkg/errors"
	"github.com/reddec/symbols"
	"io/ioutil"
)

var tpName = flag.String("type", "", "Type name to wrap")
var pkName = flag.String("package", "", "Output package (default: same as in input file)")
var ouName = flag.String("out", "", "Output file (default: <type name>_storage.go)")

func main() {
	flag.Parse()
	if *tpName == "" {
		panic("type name should be specified")
	}

	if *ouName == "" {
		*ouName = snaker.CamelToSnake(*tpName) + "_queue.go"
	}

	f, err := generate(".", *tpName, *pkName)
	if err != nil {
		panic(err)
	}
	data := &bytes.Buffer{}
	err = f.Render(data)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(*ouName, data.Bytes(), 0755)
	if err != nil {
		panic(err)
	}
}

func generate(dirName, typeName string, pkgName string) (*jen.File, error) {
	project, err := symbols.ProjectByDir(dirName)
	if err != nil {
		return nil, err
	}
	var file *jen.File
	if pkgName == "" {
		pkgName = project.Package.Package
		file = jen.NewFilePathName(project.Package.Import, pkgName)
	} else {
		file = jen.NewFile(pkgName)
	}

	sym := project.Package.FindSymbol(typeName)
	if sym == nil {
		return nil, errors.Errorf("symbol %v is not found", typeName)
	}

	file.HeaderComment("Code generated by typedqueue -type " + typeName + ". DO NOT EDIT.")
	symQual := jen.Qual(sym.Import.Import, typeName)
	stName := sym.Name + "Queue"
	file.Comment("Typed queue for " + typeName)
	file.Type().Id(stName).StructFunc(func(struc *jen.Group) {
		struc.Id("queue").Op("*").Qual("github.com/reddec/wal/mapqueue", "Queue")
	})

	file.Line()
	file.Comment("Creates new queue for " + typeName)
	file.Func().Id("New" + stName).Params(jen.Id("queue").Op("*").Qual("github.com/reddec/wal/mapqueue", "Queue")).Op("*").Id(stName).BlockFunc(func(fn *jen.Group) {
		fn.Return(jen.Op("&").Id(stName).Values(jen.Id("queue").Op(":").Id("queue")))
	})

	file.Line()
	file.Comment("Put single " + typeName + " encoded in JSON into queue")
	file.Func().Parens(jen.Id("cs").Op("*").Id(stName)).Id("Put").Params(jen.Id("item").Op("*").Add(symQual)).Params(jen.Error()).BlockFunc(func(fn *jen.Group) {
		fn.List(jen.Id("data"), jen.Err()).Op(":=").Qual("encoding/json", "Marshal").Call(jen.Id("item"))
		fn.If(jen.Err().Op("!=").Nil()).BlockFunc(func(group *jen.Group) {
			group.Return(jen.Err())
		})
		fn.Err().Op("=").Id("cs").Dot("queue").Dot("Put").Call(jen.Id("data"))
		fn.Return(jen.Err())
	})

	file.Line()
	file.Comment("Peek single " + typeName + " from queue and decode data as JSON")
	file.Func().Parens(jen.Id("cs").Op("*").Id(stName)).Id("Head").Params().Parens(jen.List(jen.Op("*").Add(symQual), jen.Error())).BlockFunc(func(fn *jen.Group) {
		fn.List(jen.Id("data"), jen.Err()).Op(":=").Id("cs").Dot("queue").Dot("Head").Call()
		fn.If(jen.Err().Op("!=").Nil()).BlockFunc(func(group *jen.Group) {
			group.Return(jen.Nil(), jen.Err())
		})
		fn.Var().Id("item").Add(symQual)
		fn.Err().Op("=").Qual("encoding/json", "Unmarshal").Call(jen.Id("data"), jen.Op("&").Id("item"))
		fn.If(jen.Err().Op("!=").Nil()).BlockFunc(func(group *jen.Group) {
			group.Return(jen.Nil(), jen.Err())
		})
		fn.Return(jen.Op("&").Id("item"), jen.Nil())
	})

	file.Line()
	file.Comment("Base (underline) queue")
	file.Func().Parens(jen.Id("cs").Op("*").Id(stName)).Id("Base").Params().Params(jen.Op("*").Qual("github.com/reddec/wal/mapqueue", "Queue")).BlockFunc(func(fn *jen.Group) {
		fn.Return(jen.Id("cs").Dot("queue"))
	})

	return file, nil
}
