package com.apress.todo.controller;

import lombok.AllArgsConstructor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.apress.todo.domain.Order;
import com.apress.todo.service.OrderService;

import javax.validation.constraints.NotNull;
import java.util.UUID;

@RestController
@AllArgsConstructor
public class OrderController {

    @Autowired
	private final OrderService orderService;

    @PostMapping("order")
    public Order placeOrder(@RequestBody @NotNull(message = "Invalid Order") Order order) {
        return orderService.placeOrder().apply(order);
    }

    @GetMapping("order/status/{orderUuid}")
    public OrderStatus statusCheck(@PathVariable("orderUuid") UUID orderUuid) {
        return orderService.statusCheck().apply(orderUuid);
    }

}
