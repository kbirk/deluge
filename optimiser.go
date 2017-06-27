package deluge

import (
	"time"

	"github.com/unchartedsoftware/deluge/progress"
	"github.com/unchartedsoftware/plog"
)

// Solution interface represents an optimisable solution.
type Solution interface {
	Score() float64
	GetValue() int64
	SetValue(int64)
}

// Optimiser interface is an algorithm that can optimise a solution.
type Optimiser interface {
	Optimise(Solution)
}

// BulkSize type implements the Solution interface to optimise the bulk size.
type BulkSize struct {
	Ingestor *Ingestor
}

// NewBulkSize will create a new BulkSize.
func NewBulkSize(ingestor *Ingestor) *BulkSize {
	return &BulkSize{
		Ingestor: ingestor,
	}
}

// Score will provide a score for the bulk size by getting the docs / second
// processed over a 30 second period.
func (bs *BulkSize) Score() float64 {
	startDocCount := progress.GetDocCount()
	time.Sleep(time.Second * 30)
	endDocCount := progress.GetDocCount()

	return float64(endDocCount-startDocCount) / 30
}

// GetValue returns the current bulk size.
func (bs *BulkSize) GetValue() int64 {
	return bs.Ingestor.bulkByteSize
}

// SetValue set the bulk size.
func (bs *BulkSize) SetValue(value int64) {
	log.Infof("Setting bulk byte size to %d", value)
	bs.Ingestor.bulkByteSize = value
}

// HillClimber implements the Optimiser interface.
type HillClimber struct {
	acceleration float64
	step         float64
	epsilon      int64
	minValue     int64
	maxValue     int64
}

// NewHillClimber creates a new HillClimber instance.
func NewHillClimber(acceleration, step float64, epsilon int64, minValue, maxValue int64) *HillClimber {
	return &HillClimber{
		acceleration: acceleration,
		step:         step,
		epsilon:      epsilon,
	}
}

// Optimise will optimise the solution using a basic hill climbing approach.
func (hc *HillClimber) Optimise(solution Solution) {
	bestScore := solution.Score()

	// Want to test both sides of the current value.
	accelerationAdjustments := []float64{-1, 1}

	log.Infof("Starting optimization run.")
	for int64(hc.step) > hc.epsilon {
		currentValue := solution.GetValue()

		bestValue := currentValue
		previousScore := bestScore
		log.Infof("Current score: %f\tCurrent value: %d", bestScore, currentValue)
		for _, aa := range accelerationAdjustments {
			value := currentValue + int64(hc.step*hc.acceleration*aa)
			value = hc.keepInBounds(value)

			solution.SetValue(value)
			score := solution.Score()
			log.Infof("Value %d got score of %f", value, score)

			if score > bestScore {
				bestScore = score
				bestValue = value
			}
		}

		if bestScore <= previousScore {
			// Old score was better so keep the value and decrease the step.
			hc.step = hc.step / hc.acceleration
			log.Infof("No difference in score. Setting step to %f", hc.step)
			solution.SetValue(currentValue)
			bestScore = solution.Score()
		} else {
			// Increase the step and set the new value.
			log.Infof("New best score. Updating value to %d", bestValue)
			solution.SetValue(bestValue)
			hc.step = hc.step * hc.acceleration
		}
	}
	log.Infof("Done optimization run.")
}

func (hc *HillClimber) keepInBounds(value int64) int64 {
	// Make sure the values fall within the bounds allowed.
	if value < hc.minValue {
		return hc.minValue
	} else if value > hc.maxValue {
		return hc.maxValue
	}

	return value
}
