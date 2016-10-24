package cola

import (
	"fmt"
	"sync"
	"time"
)

// Objeto que guarda: nombre del segmento, duracion del mismo en segundos
// solamente el timeout borra segmentos de la lista durante el Keeping() siempre de arriba
type Cola struct {
	timestamp map[string]int64   // segmento, timestamp
	segmento  map[string]float64 // segmento, duracion
	orden     []string           // segmentos en orden
	index     int                // puntero al segmento actual a bajar (Next) =0 (singularidad)
	timeout   int64
	length    int				// logitud de la cola en este momento (Len)
	mu_pls    sync.Mutex
}

func CreateQueue(timeout int64) *Cola {
	cola := &Cola{}
	cola.mu_pls.Lock()
	defer cola.mu_pls.Unlock()

	cola.timeout = timeout
	cola.index = 0
	cola.length = 0
	cola.segmento = make(map[string]float64)
	cola.timestamp = make(map[string]int64)
	cola.orden = []string{}

	return cola
}

// añade segmentos nuevos al final de la cola y les pone su tiempo de entrada
func (c *Cola) Add(segment string, duration float64) {
	c.mu_pls.Lock()
	defer c.mu_pls.Unlock()

	// no añadimos segmentos preexistentes, solo nuevos
	_, ok := c.timestamp[segment]
	if ok {
		return
	}

	c.timestamp[segment] = time.Now().Unix()
	c.segmento[segment] = duration
	c.orden = append(c.orden, segment)
	c.length++
}

// devuelve el segmento, duracion del primero en la cola y la longitud actual de la cola
// devuelve "", 0.0 , 0
// returns: nombre_segmento, duracion_seconds, hay_siguiente?
func (c *Cola) Next() (string, float64, bool) {
	var segment string
	var duration float64
	
	c.mu_pls.Lock()
	defer c.mu_pls.Unlock()

	// cola completamente vacía sin elementos
	if c.length < 1 {
		return "", 0.0, false	
	}
	
	// el index esta justo al final de la cola, y no hay siguiente
	if c.index > (c.length - 1) {
		return "", 0.0, false
	}
	
	
	segment = c.orden[c.index]
	duration = c.segmento[segment]
	c.index++

	return segment, duration, true
}

// borra elementos de la cola caducados por el timeout
func (c *Cola) Keeping() {
	c.mu_pls.Lock()
	defer c.mu_pls.Unlock()

	if c.length < 1 { // cola vacía, nada q hacer
		return
	}

	copia := []string{}
	timelimit := time.Now().Unix() - c.timeout
	for _,v := range c.orden {
		if c.timestamp[v] < timelimit { // hay q borrarlo
			delete(c.segmento, v)
			delete(c.timestamp, v)
			if c.index > 0 { // tratamos la singularidad =0
				c.index--
			}
			c.length--
		}else{ // lo preservamos en la copia
			copia = append(copia, v)
		}
	}
	
	// vaciamos orden y la reestablecemos con la copia
	c.orden = []string{}
	for _,v := range copia {
		c.orden = append(c.orden, v)
	}
}

// imprime la cola para debug
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

