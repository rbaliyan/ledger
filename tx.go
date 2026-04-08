package ledger

import "context"

type txKey struct{}

// WithTx returns a new context carrying the given transaction value.
// SQL backends accept *sql.Tx, MongoDB backends accept mongo.Session.
//
// When a context with a transaction is passed to Store methods, the store
// participates in the caller's transaction instead of creating its own.
// This enables atomic writes across the ledger and other tables/collections.
//
// Example (SQL):
//
//	tx, _ := db.BeginTx(ctx, nil)
//	ctx = ledger.WithTx(ctx, tx)
//	store.Append(ctx, "orders", entry)  // uses caller's tx
//	tx.Commit()
//
// Example (MongoDB):
//
//	session, _ := client.StartSession()
//	session.WithTransaction(ctx, func(sc context.Context) (any, error) {
//	    ctx := ledger.WithTx(sc, session)
//	    store.Append(ctx, "orders", entry)  // uses caller's session
//	    return nil, nil
//	})
func WithTx(ctx context.Context, tx any) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// TxFromContext returns the transaction stored in the context, or nil.
// This function is intended for Store implementors.
func TxFromContext(ctx context.Context) any {
	return ctx.Value(txKey{})
}
