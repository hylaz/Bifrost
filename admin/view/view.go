package view

import "embed"

//go:embed public/*
var EmbedPublic embed.FS

//go:embed template/*
var EmbedTemplate embed.FS
