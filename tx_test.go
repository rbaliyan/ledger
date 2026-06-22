package ledger

import (
	"context"
	"testing"
)

func TestWithTx_RoundTrip(t *testing.T) {
	t.Parallel()
	type fakeTx struct{ id int }
	tx := &fakeTx{id: 7}

	ctx := WithTx(context.Background(), tx)

	got := TxFromContext(ctx)
	if got == nil {
		t.Fatal("TxFromContext returned nil after WithTx")
	}
	gotTx, ok := got.(*fakeTx)
	if !ok {
		t.Fatalf("TxFromContext returned %T, want *fakeTx", got)
	}
	if gotTx != tx {
		t.Errorf("TxFromContext = %v, want %v", gotTx, tx)
	}
}

func TestTxFromContext_Absent(t *testing.T) {
	t.Parallel()
	if got := TxFromContext(context.Background()); got != nil {
		t.Errorf("TxFromContext on bare context = %v, want nil", got)
	}
}

func TestWithTx_NilValue(t *testing.T) {
	t.Parallel()
	ctx := WithTx(context.Background(), nil)
	if got := TxFromContext(ctx); got != nil {
		t.Errorf("TxFromContext after WithTx(nil) = %v, want nil", got)
	}
}
