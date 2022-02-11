package com.luxoft.kirilin.messagetransmitter.test;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

@Data
@AllArgsConstructor
public class Unit {
    private String identity;
    private UUID uuid;
}
