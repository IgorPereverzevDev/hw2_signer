package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {

	in := make(chan interface{}, 100)
	out := make(chan interface{}, 100)

	wg := &sync.WaitGroup{}

	for _, job := range jobs {
		wg.Add(1)
		go Run(wg, job, in, out)
		in = out
		out = make(chan interface{}, 100)
	}
	wg.Wait()
}

func Run(wg *sync.WaitGroup, job job, in chan interface{}, out chan interface{}) {
	defer wg.Done()
	job(in, out)
	defer close(out)
}

func SingleHash(in, out chan interface{}) {

	mu := &sync.Mutex{}
	wgSingleHash := &sync.WaitGroup{}

	for input := range in {
		wgSingleHash.Add(1)
		go asyncCrc32SingleHash(input, out, wgSingleHash, mu)
	}
	wgSingleHash.Wait()
}

func asyncCrc32SingleHash(in interface{}, out chan interface{}, wgSingleHash *sync.WaitGroup, mu *sync.Mutex) {
	defer wgSingleHash.Done()

	value, _ := in.(int)
	data := strconv.Itoa(value)

	mu.Lock()
	md5hash := DataSignerMd5(data)
	mu.Unlock()

	mapData := map[string]string{"crc32": data, "md5hash": md5hash}

	crc32 := make(map[string]string, 2)

	wg := &sync.WaitGroup{}
	for key := range mapData {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			hash := DataSignerCrc32(mapData[key])
			mu.Lock()
			crc32[key] = hash
			mu.Unlock()
		}(key)
	}
	wg.Wait()

	mu.Lock()
	singleHash := crc32["crc32"] + "~" + crc32["md5hash"]
	mu.Unlock()

	out <- singleHash
}

func MultiHash(in, out chan interface{}) {

	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	for input := range in {
		wg.Add(1)
		go asyncCrc32MultiHash(input, out, wg, mu)
	}
	wg.Wait()
}

func asyncCrc32MultiHash(in interface{}, out chan interface{}, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done()

	value, _ := in.(string)
	wgMultiHash := &sync.WaitGroup{}

	crc32Data := make(map[int]string, 6)

	for th := 0; th < 6; th++ {
		wgMultiHash.Add(1)
		go func(mData map[int]string, th int, value string) {
			defer wgMultiHash.Done()
			hash := DataSignerCrc32(strconv.Itoa(th) + value)
			mu.Lock()
			mData[th] = hash
			mu.Unlock()
		}(crc32Data, th, value)
	}
	wgMultiHash.Wait()

	keys := make([]int, 0, len(crc32Data))

	for k := range crc32Data {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	var crc32 string
	for k := range keys {
		crc32 += crc32Data[k]
	}
	out <- crc32
}

func CombineResults(in, out chan interface{}) {
	var hashes []string
	for input := range in {
		data, _ := input.(string)
		hashes = append(hashes, data)
	}
	sort.Strings(hashes)
	result := strings.Join(hashes, "_")
	out <- result
}
