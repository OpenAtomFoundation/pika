package pika

type Server struct {
	stop chan bool
}

func New(s string) Server {
	return Server{}
}
func (s *Server) Start() {
}
func (s *Server) Loop() {

	//for {
	//	select {
	//	case stop := <-s.stop:
	//		{
	//
	//		}
	//	case msg := <- s.:
	//
	//	}
	//}
}
func (s *Server) Stop() {
	defer func() {}()
	s.stop <- true
}
func (s *Server) Close() {
	s.stop <- true
}
func (s *Server) GetMessage() {

}

func (s *Server) SendMetaSync() {

}
