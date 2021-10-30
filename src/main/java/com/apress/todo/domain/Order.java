package com.apress.todo.domain;

import lombok.*;

import javax.validation.constraints.NotBlank;

import com.apress.todo.controller.OrderStatus;

import java.util.UUID;

@ToString
@Builder
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Order {

    private UUID orderUuid;

    @NotBlank
    private String itemName;

    private OrderStatus orderStatus;
}
