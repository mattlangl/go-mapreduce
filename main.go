package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/wasmerio/wasmer-go/wasmer"
)

var wasmInstances []*wasmer.Instance
var wasmMemories []*wasmer.Memory
var wasmInstanceLocks []sync.Mutex
var wasmMemoryLocks []sync.Mutex

// InitializeWasmerInstances initializes the pool of Wasmer instances with a given Wasm file.
func initializeWasmerInstances(wasmFile []byte, poolSize int) error {
	engine := wasmer.NewEngine()
	store := wasmer.NewStore(engine)

	// Compile the module
	module, err := wasmer.NewModule(store, wasmFile)
	if err != nil {
		return err
	}

	// Create an import object
	importObject := wasmer.NewImportObject()

	for i := 0; i < poolSize; i++ {
		// Instantiate the module
		instance, err := wasmer.NewInstance(module, importObject)
		if err != nil {
			return err
		}

		wasmInstances = append(wasmInstances, instance)
		wasmInstanceLocks = append(wasmInstanceLocks, sync.Mutex{}) // Add a lock for each wasmInstance

		// Get memory
		memory, err := instance.Exports.GetMemory("memory")
		if err != nil {
			return err
		}

		wasmMemories = append(wasmMemories, memory)
		wasmMemoryLocks = append(wasmMemoryLocks, sync.Mutex{}) // Add a lock for each wasmMemory
	}

	return nil
}

func passStringToWasm(data string, pool int) (int32, int, error) {
	// Encode the string as UTF-8.
	dataBytes := []byte(data)

	// Initially allocate memory for the string.
	ptr, err := wasmMallocAndCheckGrowth(int32(len(dataBytes)), pool)
	if err != nil {
		log.Println("Error allocating memory for string:", err)
		return 0, 0, err
	}
	wasmMemoryLocks[pool].Lock()
	defer wasmMemoryLocks[pool].Unlock()
	// Copy the string data into the allocated Wasm memory.
	copy(wasmMemories[pool].Data()[ptr:], dataBytes)

	return ptr, len(dataBytes), nil
}

// getStringFromWasm gets a string from Wasm memory based on a pointer and length.
func getStringFromWasm(ptr int32, length int, pool int) (string, error) {
	// Retrieve the byte slice from Wasm memory.
	log.Println("Getting data from pointer", ptr, "with length", length)
	data := wasmMemories[pool].Data()[ptr : ptr+int32(length)]

	// Check if the byte slice is valid UTF-8.
	if !utf8.Valid(data) {
		log.Println("Data at pointer", ptr, "with length", length, "is not valid UTF-8")
		return "", fmt.Errorf("data at pointer %d with length %d is not valid UTF-8", ptr, length)
	}

	// Convert the byte slice to a string and return.

	return string(data), nil
}

// Allocate memory in Wasm for the return value pointer
func allocateReturnValueSpace(pool int) (int32, error) {
	ptr, err := wasmMallocAndCheckGrowth(8, pool) // Assuming 1 for alignment; adjust as needed.
	if err != nil {
		log.Println("Error allocating memory for return value pointer:", err)
		return 0, err
	}

	return ptr, nil
}

func readReturnValuesFromPointer(pointer int32, pool int) (resultPointer int32, resultLength int32) {
	wasmMemoryLocks[pool].Lock()
	defer wasmMemoryLocks[pool].Unlock()

	memoryData := wasmMemories[pool].Data()
	resultPointer = int32(binary.LittleEndian.Uint32(memoryData[pointer : pointer+4]))
	resultLength = int32(binary.LittleEndian.Uint32(memoryData[pointer+4 : pointer+8]))
	return resultPointer, resultLength
}

func wasmMallocAndCheckGrowth(size int32, pool int) (int32, error) {
	log.Println("Allocating", size, "bytes of memory in Wasm")
	// Lock the memory growth logic to
	// prevent

	ptr, err := callFunction("__wbindgen_malloc", pool, size, 1)
	if err != nil {
		log.Println("Error calling malloc, attempting to grow memory:", err)
		wasmMemoryLocks[pool].Lock()
		defer wasmMemoryLocks[pool].Unlock()
		// Calculate how many pages are needed to accommodate the new allocation.
		// Each page is 64KB, so size needs to be converted to pages.
		pagesNeeded := wasmer.Pages((size + 65535 - 1) / 65536)
		if !wasmMemories[pool].Grow(pagesNeeded) {
			log.Printf("Failed to grow memory by %d pages", pagesNeeded)
			return 0, fmt.Errorf("failed to grow memory by %d pages", pagesNeeded)
		}

		// Allocate memory using the Wasm module's malloc.
		ptr, err = callFunction("__wbindgen_malloc", pool, size, 1)
		if err != nil {
			log.Println("Error calling malloc again:", err)
			return 0, err
		}
	}

	// Ensure the returned pointer is an int32 and validate its range if needed.
	ptrInt32, ok := ptr.(int32)
	if !ok {
		log.Println("Memory allocation did not return a valid int32 pointer.")
		return 0, fmt.Errorf("invalid pointer returned from memory allocation")
	}

	return ptrInt32 >> 0, nil
}

func callFunction(name string, pool int, args ...interface{}) (interface{}, error) {
	wasmInstanceLocks[pool].Lock()
	defer wasmInstanceLocks[pool].Unlock()

	function, err := wasmInstances[pool].Exports.GetFunction(name)
	if err != nil {
		return nil, err
	}

	return function(args...)
}

// callWasmFunction is a general-purpose function to call a Wasm function that expects a string and returns a string.
func callWasmFunction(functionName string, data string, pool int) (string, error) {
	log.Println("Calling Wasm function", functionName)

	ptr, len, err := passStringToWasm(data, pool)
	if err != nil {
		log.Println("Error passing string to Wasm:", err)
		return "", err
	}

	returnValuePtr, err := allocateReturnValueSpace(pool)
	if err != nil {
		log.Println("Error allocating memory for return value pointer:", err)
		return "", err
	}

	_, err = callFunction("map_function", pool, returnValuePtr, ptr, len)
	if err != nil {
		log.Println("Error calling Wasm function:", err)
		return "", err
	}

	resultPtr, resultPtrlen := readReturnValuesFromPointer(returnValuePtr, pool)

	_, err = callFunction("__wbindgen_free", pool, returnValuePtr, 8, 1)
	if err != nil {
		log.Println("Error freeing memory:", err)
		return "", err
	}

	result, err := getStringFromWasm(resultPtr, int(resultPtrlen), pool)
	if err != nil {
		log.Println("Error getting string from Wasm:", err)
		return "", err
	}

	_, err = callFunction("__wbindgen_free", pool, resultPtr, resultPtrlen, 1)
	if err != nil {
		log.Println("Error freeing memory:", err)
		return "", err
	}

	return result, nil
}

func mapFunction(data string) string {
	words := strings.FieldsFunc(data, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	})

	counts := make(map[string]int)
	for _, word := range words {
		lowercaseWord := strings.ToLower(word)
		counts[lowercaseWord]++
	}

	wordCounts := make(map[string]string)
	for key, value := range counts {
		wordCounts[key] = strconv.Itoa(value)
	}

	result, err := json.Marshal(wordCounts)
	if err != nil {
		return "{}"
	}

	return string(result)
}

func reduceFunction(data string) string {
	var values []string
	err := json.Unmarshal([]byte(data), &values)
	if err != nil {
		return "0"
	}

	sum := 0
	for _, val := range values {
		intVal, err := strconv.Atoi(val)
		if err != nil {
			continue
		}
		sum += intVal
	}

	return strconv.Itoa(sum)
}

func main() {
	// Load the WebAssembly module
	wasmBytes, err := os.ReadFile("/Users/mattlangl/Desktop/dev/rust-wasm-mapreduce-example/pkg/rust_wasm_mapreduce_example_bg.wasm")
	if err != nil {
		fmt.Println("Error reading Wasm file:", err)
		return
	}

	// Initialize the Wasmer instance
	err = initializeWasmerInstances(wasmBytes, 10)
	if err != nil {
		fmt.Println("Error initializing Wasmer instance:", err)
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(fileIndex int) {
			defer wg.Done()

			fileName := fmt.Sprintf("1342-%d.txt", i/250)
			file, err := os.ReadFile(fileName)
			if err != nil {
				fmt.Printf("Error reading file %s: %v\n", fileName, err)
				return
			}

			result, err := callWasmFunction("map_function", string(file), i%10)
			if err != nil {
				fmt.Printf("Error calling Wasm function for file %s: %v\n", fileName, err)
				return
			}

			// _ = mapFunction(string(file))
			fmt.Printf("Result for file %s partition %d received %s\n", fileName, i, result) // Placeholder for actual result handling

		}(i)
	}

	wg.Wait()
}
