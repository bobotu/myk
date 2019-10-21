module github.com/bobotu/myk

go 1.12

require (
	github.com/brianvoe/gofakeit v3.18.0+incompatible
	github.com/coocood/badger v1.5.1-0.20190716024607-37ea3d37efe9
	github.com/coocood/bbloom v0.0.0-20180518162752-7774d68761e5
	github.com/dgryski/go-farm v0.0.0-20190104051053-3adb47b1fb0f
	github.com/pingcap/failpoint v0.0.0-20190708053854-e7b1061e6e81
	github.com/pingcap/tidb v0.0.0-20190325083614-d6490c1cab3a
	github.com/stretchr/testify v1.3.0
)

replace github.com/stretchr/testify => github.com/bobotu/testify v1.3.1-0.20190730155233-067b303304a8
