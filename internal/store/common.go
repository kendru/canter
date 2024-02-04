package store

type Resolver interface {
	Resolve(conn *Connection) (ID, error)
}
