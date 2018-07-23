package cosmosdb

import "sort"

func meanFloat64(data []float64) float64 {
	sum := float64(0)
	for _, value := range data {
		sum += value
	}
	return sum / float64(len(data))
}

func meanInt64(data []int64) float64 {
	sum := int64(0)
	for _, value := range data {
		sum += value
	}
	return float64(sum) / float64(len(data))
}

func medianFloat64(data []float64) float64 {
	sort.Float64s(data)
	length := len(data)

	if length%2 == 0 {
		return meanFloat64(data[length/2-1 : length/2+1])
	}

	return float64(data[length/2])
}
