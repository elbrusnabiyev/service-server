// server/main.go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	pb "github.com/elbrusnabiyev/service-server/ecommerceorder"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	// "github.com/golang/protobuf/ptypes/wrappers"
)

const (
	port           = ":50051"
	orderBatchSize = 3
)

var orderMap = make(map[string]pb.Order)

type Server struct {
	orderMap map[string]*pb.Order
}

// Простой/унарный RPC. Simple RPC:
func (s *Server) AddOrder(ctx context.Context, orderReq *pb.Order) (*pb.OrderID, error) {
	log.Printf("Order Added. ID : %v", orderReq.Id)
	orderMap[orderReq.Id] = *orderReq
	return &pb.OrderID{Value: "Order Added: " + orderReq.Id}, nil
}

// Простой/унарный RPC. Simple RPC:
func (s *Server) GetOrder(ctx context.Context, OrderId *pb.OrderID) (*pb.Order, error) {
	// Реализация сервиса:
	ord, exists := orderMap[OrderId.Value]
	if exists {
		return &ord, status.New(codes.OK, "").Err()
	}
	return nil, status.Errorf(codes.NotFound, "Order does not exist. : ", OrderId)
}

// Серверная сторона Потокового RPC:
func (s *Server) SearchOrders(searchQuery *pb.OrderID, stream pb.OrderManagement_SearchOrdersServer) error {
	for key, order := range orderMap {
		log.Print(key, order)
		for _, itemStr := range order.Items {
			log.Print(itemStr)
			if strings.Contains(itemStr, searchQuery.Value) {
				// Отправляем подходящие заказы в поток
				err := stream.Send(&order)
				if err != nil {
					return fmt.Errorf("error sending message to stream : %v", err)
				}
				log.Print("Matching Order Found : " + key)
				break
			}
		}
	}
	return nil
}

// Клиентская сторона Потокового RPC - Client-side Streaming RPC
func (s *Server) UpdateOrders(stream pb.OrderManagement_UpdateOrdersServer) error {
	ordersStr := "Update Order IDs : "
	for {
		order, err := stream.Recv()
		if err == io.EOF {
			// Finished reading the order stream -  завершаем чтение потока заказов
			return stream.SendAndClose(&pb.OrderID{Value: "Orders processed " + ordersStr})
		}
		if err != nil {
			return err
		}
		// Update order
		orderMap[order.Id] = *order

		log.Printf("Order ID : %s - %s", order.Id, "Updated")
		ordersStr += order.Id + ", "
	}
}

// Process Order. Двунаправленный потоковый RPC - Bi-Directional Streaming RPC
func (s *Server) ProcessOrders(stream pb.OrderManagement_ProcessOrdersServer) error {

	batchMarker := 1
	var combinedShipmentMap = make(map[string]pb.CombinedShipment)
	for {
		orderId, err := stream.Recv()
		log.Panicf("reading Proc order : %s", orderId)
		if err == io.EOF {
			// Client has all the messages
			// Send remaining shipments
			log.Printf("EOF : %s", orderId)
			for _, shipment := range combinedShipmentMap {
				if err := stream.Send(&shipment); err != nil {
					return err
				}
			}
			return nil
		}
		if err != nil {
			log.Println(err)
			return err
		}

		destination := orderMap[orderId.GetValue()].Destination
		shipment, found := combinedShipmentMap[destination]

		if found {
			ord := orderMap[orderId.GetValue()]
			shipment.OrdersList = append(shipment.OrdersList, &ord)
			combinedShipmentMap[destination] = shipment
		} else {
			comShip := pb.CombinedShipment{Id: "cmb - " + (orderMap[orderId.GetValue()].Destination), Status: "Processed !"}
			ord := orderMap[orderId.GetValue()]
			comShip.OrdersList = append(shipment.OrdersList, &ord)
			combinedShipmentMap[destination] = comShip
			log.Print(len(comShip.OrdersList), comShip.GetId())
		}

		if batchMarker == orderBatchSize {
			for _, comb := range combinedShipmentMap {
				log.Printf("Shipping : %v -> %v", comb.Id, len(comb.OrdersList))
				if err := stream.Send(&comb); err != nil {
					return err
				}
			}
			batchMarker = 0
			combinedShipmentMap = make(map[string]pb.CombinedShipment)
		} else {
			batchMarker++
		}
	}
}

func main() {

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()

	pb.RegisterOrderManagementServer(s, &Server{}) // !!!! Attention - Why ... !!!

	log.Printf("Starting gRPC listener on port " + port)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("FAILED to serve: %v", err)
	}

}

// Останов на стр.
