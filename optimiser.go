package deluge

import (
	"time"

	"github.com/unchartedsoftware/deluge/progress"
	"github.com/unchartedsoftware/plog"
)

const (
	scoringDuration     = 30
	numberOfRuns        = 3
	defaultAcceleration = float64(1.5)
	defaultStep         = float64(1024 * 1024 * 2)
	defaultEpsilon      = int64(1024 * 500)
	defaultMaxStep      = int64(1024 * 1024 * 4)
	defaultMinValue     = int64(1024 * 1024)
	defaultMaxValue     = int64(1024 * 1024 * 60)
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
// processed over a defined period.
func (bs *BulkSize) Score() float64 {
	startDocCount := progress.GetDocCount()
	time.Sleep(time.Second * scoringDuration)
	endDocCount := progress.GetDocCount()

	return float64(endDocCount-startDocCount) / scoringDuration
}

// GetValue returns the current bulk size.
func (bs *BulkSize) GetValue() int64 {
	return bs.Ingestor.getBulkByteSize()
}

// SetValue set the bulk size.
func (bs *BulkSize) SetValue(value int64) {
	log.Infof("Setting bulk byte size to %d", value)
	bs.Ingestor.setBulkByteSize(value)
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
func NewHillClimber(options ...HillClimberOptionFunc) (*HillClimber, error) {
	hc := &HillClimber{
		acceleration: defaultAcceleration,
		step:         defaultStep,
		epsilon:      defaultEpsilon,
		minValue:     defaultMinValue,
		maxValue:     defaultMaxValue,
	}

	for _, option := range options {
		if err := option(hc); err != nil {
			return nil, err
		}
	}

	return hc, nil
}

// Optimise will optimise the solution using a basic hill climbing approach.
func (hc *HillClimber) Optimise(solution Solution) {
	// Want to test both sides of the current value.
	accelerationAdjustments := []float64{-1, 1}

	log.Infof("Starting optimization run.")
	for int64(hc.step) > hc.epsilon {
		// Track the winners of the run. Overall best solution for iteration
		// will be the adjustment that has won the most runs.
		runResult := make([]uint, 3)
		currentValue := solution.GetValue()

		// Run it a few times since throughput can fluctuate a lot.
		// NOTE: bestAA is the value of aa that generates the best score. It
		// can be -1, 0 (current value) or 1.
		for runCount := 0; runCount < numberOfRuns; runCount++ {
			log.Infof("Run #%d", runCount)
			bestScore := solution.Score()
			bestAA := 0

			log.Infof("Current score: %f\tCurrent value: %d", bestScore, currentValue)
			for _, aa := range accelerationAdjustments {
				value := currentValue + int64(hc.step*hc.acceleration*aa)
				value = hc.keepInBounds(value)

				solution.SetValue(value)
				score := solution.Score()
				log.Infof("Value %d got score of %f", value, score)

				if score > bestScore {
					bestScore = score
					bestAA = int(aa)
				}
			}

			log.Infof("Run #%d winner: %d", runCount, bestAA)
			runResult[bestAA+1]++
			solution.SetValue(currentValue)

			// Shortcut if clear winner has emerged.
			if runResult[bestAA+1] > numberOfRuns/2 {
				break
			}
		}

		winner := hc.findWinner(runResult)
		winner = winner - 1
		if winner == 0 {
			// Old score was better so keep the value and decrease the step.
			hc.step = hc.step / hc.acceleration
			log.Infof("No difference in score. Setting step to %f", hc.step)
			solution.SetValue(currentValue)
		} else {
			// Increase the step and set the new value.
			newValue := currentValue + int64(hc.step*hc.acceleration*float64(winner))
			newValue = hc.keepInBounds(newValue)
			log.Infof("New best score. Updating value to %d", newValue)
			solution.SetValue(newValue)
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

func (hc *HillClimber) findWinner(runResults []uint) int {
	bestIndex := -1
	bestValue := uint(0)
	for index, value := range runResults {
		if value > bestValue {
			bestValue = value
			bestIndex = index
		}
	}

	return bestIndex
}

// HillClimberOptionFunc is a function that configures a HillClimber. It is used
// in NewHillClimber.
type HillClimberOptionFunc func(*HillClimber) error

// SetAcceleration sets the acceleration used by the optimiser.
func SetAcceleration(acceleration float64) HillClimberOptionFunc {
	return func(hc *HillClimber) error {
		hc.acceleration = acceleration
		return nil
	}
}

// SetStep sets the step used by the optimiser.
func SetStep(step float64) HillClimberOptionFunc {
	return func(hc *HillClimber) error {
		hc.step = step
		return nil
	}
}

// SetEpsilon sets the epsilon used by the optimiser.
func SetEpsilon(epsilon int64) HillClimberOptionFunc {
	return func(hc *HillClimber) error {
		hc.epsilon = epsilon
		return nil
	}
}

// SetMinValue sets the minimum value used by the optimiser.
func SetMinValue(minValue int64) HillClimberOptionFunc {
	return func(hc *HillClimber) error {
		hc.minValue = minValue
		return nil
	}
}

// SetMaxValue sets the maximum value used by the optimiser.
func SetMaxValue(maxValue int64) HillClimberOptionFunc {
	return func(hc *HillClimber) error {
		hc.maxValue = maxValue
		return nil
	}
}
