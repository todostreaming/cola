package cola

import (
	"fmt"
	"sync"
	"time"
)

type Cola struct {
	timestamp map[string]int64   // segmento, timestamp
	segmento  map[string]float64 // segmento, duracion
	orden     []string           // segmentos en orden
	index     int                // puntero del Next al segmento actual a recoger
	timeout   int64
	mu_pls    sync.Mutex
}

func CreateQueue(timeout int64) *Cola {
	cola := &Cola{}
	cola.mu_pls.Lock()
	defer cola.mu_pls.Unlock()

	cola.timeout = timeout
	cola.index = 0
	cola.segmento = make(map[string]float64)
	cola.timestamp = make(map[string]int64)

	return cola
}

func (c *Cola) Add(segment string, duration float64) {
	c.mu_pls.Lock()
	defer c.mu_pls.Unlock()

	_, ok := c.timestamp[segment]
	if ok {
		return
	}

	c.timestamp[segment] = time.Now().Unix()
	c.segmento[segment] = duration
	c.orden = append(c.orden, segment)
}

func (c *Cola) Next() (string, float64) {
	var segment string
	var duration float64

	c.mu_pls.Lock()
	defer c.mu_pls.Unlock()

	if len(c.orden) < 1 {
		return "",0.0
	}
	if len(c.orden)-1 < c.index {
		return "",0.0
	}
	segment = c.orden[c.index]
	duration = c.segmento[segment]
	c.index++

	return segment, duration
}

// Esta funcion mantiene los timestamp dentro de un valor timeout
func (c *Cola) Keeping() {
	c.mu_pls.Lock()
	defer c.mu_pls.Unlock()
	
	if len(c.orden) < 1 {
		return
	}
	copia := []string{}
	for _, v := range c.orden {
		copia = append(copia, v)
	}
	now := time.Now().Unix()
	deleted := 0
	for _, s := range copia {
		tiempo := now - c.timestamp[s]
		if tiempo > c.timeout {
			c.orden = c.orden[1:]
			delete(c.segmento, s)
			delete(c.timestamp, s)
			deleted++
		}
	}
	if len(c.orden)-1 < c.index {
		c.index = c.index - deleted
		return
	}
	found := false
	index_segment := c.orden[c.index]
	for i, s := range c.orden {
		if index_segment == s {
			found = true
			c.index = i
			break
		}
	}
	if !found {
		c.index = 0
	}
}

func (c *Cola) Len() int {
	c.mu_pls.Lock()
	defer c.mu_pls.Unlock()
	
	return len(c.orden)
}

func (c *Cola) Print() {
	c.mu_pls.Lock()
	defer c.mu_pls.Unlock()

	fmt.Println("=================================================================")
	fmt.Printf("Orden\tSegmento\tDuración\tTimestamp\n")
	for i, s := range c.orden {
		if i == c.index {
			fmt.Printf("[%d]\t%s\t\t%.2f\t\t%d\n", i, s, c.segmento[s], c.timestamp[s])
		}else{
			fmt.Printf("%d\t%s\t\t%.2f\t\t%d\n", i, s, c.segmento[s], c.timestamp[s])
		}
	}
	fmt.Println("=================================================================")
}
