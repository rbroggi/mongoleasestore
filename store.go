package mongoleasestore

import (
	"context"
	"errors"
	"time"

	le "github.com/rbroggi/leaderelection"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Store implements a lease store using MongoDB.
type Store struct {
	collection *mongo.Collection
	leaseKey   string // Unique key for the lease.
}

type Args struct {
	LeaseCollection *mongo.Collection
	LeaseKey        string
}

// NewStore creates a new Store.
func NewStore(args Args) (*Store, error) {
	store := &Store{
		collection: args.LeaseCollection,
		leaseKey:   args.LeaseKey,
	}

	return store, nil
}

// GetLease retrieves the current lease. Should return ErrLeaseNotFound if the
// lease does not exist.
func (s *Store) GetLease(ctx context.Context) (*le.Lease, error) {
	filter := bson.M{"_id": s.leaseKey}

	var doc leaseDocument
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, le.ErrLeaseNotFound
		}
		return nil, err
	}

	return doc.toLease(), nil
}

// UpdateLease updates the lease if the lease exists.
func (s *Store) UpdateLease(ctx context.Context, newLease *le.Lease) error {
	filter := bson.M{"_id": s.leaseKey}
	update := bson.M{"$set": fromLease(s.leaseKey, newLease)}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	if result.ModifiedCount == 0 {
		return le.ErrLeaseNotFound
	}

	return nil
}

// CreateLease creates a new lease if one does not exist.
func (s *Store) CreateLease(ctx context.Context, newLease *le.Lease) error {
	_, err := s.collection.InsertOne(ctx, fromLease(s.leaseKey, newLease))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return errors.New("lease already exists")
		}
		return err
	}

	return nil
}

type leaseDocument struct {
	ID                string        `bson:"_id"`
	HolderIdentity    string        `bson:"holder_identity"`
	AcquireTime       time.Time     `bson:"acquire_time"`
	RenewTime         time.Time     `bson:"renew_time"`
	LeaseDuration     time.Duration `bson:"lease_duration"`
	LeaderTransitions uint32        `bson:"leader_transitions"`
}

func (ld *leaseDocument) toLease() *le.Lease {
	return &le.Lease{
		HolderIdentity:    ld.HolderIdentity,
		AcquireTime:       ld.AcquireTime,
		RenewTime:         ld.RenewTime,
		LeaseDuration:     ld.LeaseDuration,
		LeaderTransitions: ld.LeaderTransitions,
	}
}

func fromLease(id string, lease *le.Lease) leaseDocument {
	return leaseDocument{
		ID:                id,
		HolderIdentity:    lease.HolderIdentity,
		AcquireTime:       lease.AcquireTime,
		RenewTime:         lease.RenewTime,
		LeaseDuration:     lease.LeaseDuration,
		LeaderTransitions: lease.LeaderTransitions,
	}
}
