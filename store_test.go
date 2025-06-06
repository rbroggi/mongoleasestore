package mongoleasestore

import (
	"context"
	"log"
	"testing"
	"time"

	le "github.com/rbroggi/leaderelection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestLeaderElection(t *testing.T) {
	t.Parallel()

	mongoClient := setupMongoContainer(t)
	database := mongoClient.Database(t.Name())
	collection := database.Collection(t.Name())

	// --- Test Configuration ---
	leaseDuration := 1 * time.Second
	retryPeriod := 200 * time.Millisecond
	candidates := []string{
		"candidate-1",
		"candidate-2",
		"candidate-3",
	}

	store, err := NewStore(Args{
		LeaseCollection: collection,
		LeaseKey:        "test-lease-key",
	})
	require.NoError(t, err, "Failed to create Redis lease store")

	// --- Test Context ---
	electors := make(map[string]leaderAndCnl) // Keep track of electors for later shutdown
	// --- Scenario 1: No Leader, First Leader Election ---
	t.Run("Initial Election", func(t *testing.T) {
		for _, candidateID := range candidates {
			config := le.ElectorConfig{
				LeaseDuration:   leaseDuration,
				RetryPeriod:     retryPeriod,
				LeaseStore:      store,
				CandidateID:     candidateID,
				ReleaseOnCancel: true, // Important for later scenarios.
			}
			elector, err := le.NewElector(config)
			require.NoError(t, err, "Failed to create elector")
			ctx, cancel := context.WithCancel(context.Background())
			ch := elector.Run(ctx)
			electors[candidateID] = leaderAndCnl{
				cancel:  cancel,
				elector: elector,
				done:    ch,
			}
		}

		require.Eventually(t, func() bool {
			leadersCount := countLeaders(electors)
			return leadersCount == 1
		}, 2*retryPeriod, 100*time.Millisecond, "There should be exactly one leader [%d] found", countLeaders(electors))

		require.Neverf(t, func() bool {
			leadersCount := countLeaders(electors)
			return leadersCount != 1
		}, 2*retryPeriod, 100*time.Millisecond, "There should be exactly one leader")
	})

	// --- Scenario 2: Graceful Shutdown and Leader Takeover ---
	t.Run("Graceful Shutdown and Takeover", func(t *testing.T) {
		// Find current leader
		currentLeader := findLeader(electors)
		require.NotEmpty(t, currentLeader, "There must be a leader for this test")

		log.Printf("Current leader before shutdown: %s", currentLeader)

		// Simulate graceful shutdown of current leader.
		// find index of the current leader elector
		electors[currentLeader].cancel() // Stop the elector for the current leader.

		// Eventually elector stops (done)
		assert.Eventually(t, func() bool {
			// Check if the elector is done
			select {
			case <-electors[currentLeader].done:
				return true
			default:
				return false
			}
		}, 2*retryPeriod, 100*time.Millisecond)

		// Check New Leader
		assert.Eventually(t, func() bool {
			c := countLeaders(electors)
			newLeader := findLeader(electors)
			return newLeader != "" && newLeader != currentLeader && c == 1
		}, 2*leaseDuration, 100*time.Millisecond,
			"A single new leader should be elected after shutdown. Old leader %s, new leader %s [count: %d]",
			currentLeader, findLeader(electors), countLeaders(electors))
	})

	// cancel all electors to clean up
	for _, elector := range electors {
		elector.cancel()
	}
	// Eventually all electors stop (done)
	assert.Eventually(t, func() bool {
		// Check if the elector is done
		for _, elector := range electors {
			select {
			case <-elector.done:
				continue
			default:
				return false
			}
		}
		return true
	}, 2*retryPeriod, 100*time.Millisecond)

	// Eventually no leader
	assert.Eventually(t, func() bool {
		newLeader := findLeader(electors)
		return newLeader == ""
	}, 4*time.Second, 100*time.Millisecond,
		"All leaders should be stopped and there should be no leader after shutdown")

	//--- Scenario 3: Lease Expiration and Takeover ---
	t.Run("Lease Expiration and Takeover", func(t *testing.T) {
		for _, candidateID := range candidates {
			config := le.ElectorConfig{
				LeaseDuration:   leaseDuration,
				RetryPeriod:     retryPeriod,
				LeaseStore:      store,
				CandidateID:     candidateID,
				ReleaseOnCancel: false,
			}

			elector, err := le.NewElector(config)
			require.NoError(t, err, "Failed to create elector")
			ctx, cancel := context.WithCancel(context.Background())
			ch := elector.Run(ctx)
			electors[candidateID] = leaderAndCnl{
				cancel:  cancel,
				elector: elector,
				done:    ch,
			}
		}

		require.Eventually(t, func() bool {
			leadersCount := countLeaders(electors)
			return leadersCount == 1
		}, 3*retryPeriod, 100*time.Millisecond, "There should be exactly one leader [%d found]", countLeaders(electors))

		// Find Current Leader
		currentLeader := findLeader(electors)
		require.NotEmpty(t, currentLeader, "There must be a leader for this test")
		log.Printf("Current leader before lease expiration: %s", currentLeader)
		// cancel leader
		electors[currentLeader].cancel() // Stop the elector for the current leader.

		// Wait for the elector to stop
		// Eventually elector stops (done)
		assert.Eventually(t, func() bool {
			// Check if the elector is done
			select {
			case <-electors[currentLeader].done:
				return true
			default:
				return false
			}
		}, 2*retryPeriod, 100*time.Millisecond)

		assert.Eventually(t, func() bool {
			newLeader := findLeader(electors)
			return newLeader != "" && newLeader != currentLeader
		}, 2*leaseDuration, 100*time.Millisecond, "A new leader should be elected after shutdown")

		// cancel all electors to clean up
		for _, elector := range electors {
			elector.cancel()
		}
		// Wait for all electors to stop
		// Eventually all electors stop (done)
		assert.Eventually(t, func() bool {
			// Check if the elector is done
			for _, elector := range electors {
				select {
				case <-elector.done:
					continue
				default:
					return false
				}
			}
			return true
		}, 2*retryPeriod, 100*time.Millisecond)

		// Eventually no leader
		assert.Eventually(t, func() bool {
			newLeader := findLeader(electors)
			return newLeader == ""
		}, 2*leaseDuration, 100*time.Millisecond, "A new leader should be elected after shutdown")
	})
}

// setupMongoContainer sets up a MongoDB container using testcontainers-go,
// initializes a MongoDB client, and registers a graceful shutdown.
func setupMongoContainer(t *testing.T) *mongo.Client {
	t.Helper()

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mongo:latest",
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor:   wait.ForListeningPort("27017/tcp"),
	}

	mongoContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start mongo container: %v", err)
	}

	mappedPort, err := mongoContainer.MappedPort(ctx, "27017/tcp")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}

	hostIP, err := mongoContainer.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	mongoURI := "mongodb://" + hostIP + ":" + mappedPort.Port()

	clientOptions := options.Client().ApplyURI(mongoURI)

	mongoClient, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		t.Fatalf("failed to connect to mongo: %v", err)
	}

	// Ping the MongoDB server to ensure it's up and running.
	if err := mongoClient.Ping(ctx, nil); err != nil {
		t.Fatalf("failed to ping mongo: %v", err)
	}

	// Register a cleanup function to stop the container and close the client.
	t.Cleanup(func() {
		if err := mongoClient.Disconnect(ctx); err != nil {
			t.Logf("failed to disconnect mongo client: %v", err)
		}
		if err := mongoContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate mongo container: %v", err)
		}
	})

	return mongoClient
}

type leaderAndCnl struct {
	cancel  context.CancelFunc
	done    <-chan struct{}
	elector *le.Elector
}

func countLeaders(electors map[string]leaderAndCnl) int {
	// count electors that are leading
	leadersCount := 0
	for _, elector := range electors {
		if elector.elector.IsLeader() {
			leadersCount++
		}
	}
	return leadersCount
}

func findLeader(electors map[string]leaderAndCnl) string {
	for candidate, elector := range electors {
		if elector.elector.IsLeader() {
			return candidate
		}
	}
	return ""
}
