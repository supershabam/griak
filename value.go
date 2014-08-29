package griak

// DtValue is returned from TypeBucket.GetValue() and should be further type
// checked to see if it is an implementation of a Counter Set or Map
type Value interface{}
